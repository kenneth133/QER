use QER
go
IF OBJECT_ID('dbo.rpt_stock_view_current') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rpt_stock_view_current
    IF OBJECT_ID('dbo.rpt_stock_view_current') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rpt_stock_view_current >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rpt_stock_view_current >>>'
END
go
CREATE PROCEDURE dbo.rpt_stock_view_current
@STRATEGY_ID int,
@BDATE datetime,
@ACCOUNT_CD varchar(32),
@MODEL_WEIGHT varchar(16),
@IDENTIFIER_TYPE varchar(32),
@IDENTIFIER_VALUE varchar(64),
@DEBUG bit = NULL
AS
/* STOCK - CURRENT RANKS */

IF @STRATEGY_ID IS NULL
  BEGIN SELECT 'ERROR: @STRATEGY_ID IS A REQUIRED PARAMETER' RETURN -1 END
IF @BDATE IS NULL
  BEGIN SELECT 'ERROR: @BDATE IS A REQUIRED PARAMETER' RETURN -1 END
IF @ACCOUNT_CD IS NULL
  BEGIN SELECT 'ERROR: @ACCOUNT_CD IS A REQUIRED PARAMETER' RETURN -1 END
IF @MODEL_WEIGHT IS NULL
  BEGIN SELECT 'ERROR: @MODEL_WEIGHT IS A REQUIRED PARAMETER' RETURN -1 END
IF @MODEL_WEIGHT NOT IN ('CAP', 'EQUAL')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @MODEL_WEIGHT PARAMETER' RETURN -1 END
IF @IDENTIFIER_TYPE IS NULL
  BEGIN SELECT 'ERROR: @IDENTIFIER_TYPE IS A REQUIRED PARAMETER' RETURN -1 END
IF @IDENTIFIER_TYPE NOT IN ('TICKER', 'CUSIP', 'SEDOL', 'ISIN')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @IDENTIFIER_TYPE PARAMETER' RETURN -1 END
IF @IDENTIFIER_VALUE IS NULL
  BEGIN SELECT 'ERROR: @IDENTIFIER_VALUE IS A REQUIRED PARAMETER' RETURN -1 END

CREATE TABLE #SECURITY (
  bdate			datetime		NULL,
  security_id	int				NULL,
  ticker		varchar(32)		NULL,
  cusip			varchar(32)		NULL,
  sedol			varchar(32)		NULL,
  isin			varchar(32)		NULL,
  currency_cd	varchar(3)		NULL,
  exchange_nm	varchar(60)		NULL
)

IF @IDENTIFIER_TYPE = 'TICKER'
  BEGIN INSERT #SECURITY (bdate, ticker) VALUES (@BDATE, @IDENTIFIER_VALUE) END
ELSE IF @IDENTIFIER_TYPE = 'CUSIP'
BEGIN
  INSERT #SECURITY (bdate, cusip) VALUES (@BDATE, @IDENTIFIER_VALUE)
  UPDATE #SECURITY SET cusip = equity_common.dbo.fnCusipIncludeCheckDigit(cusip)
END
ELSE IF @IDENTIFIER_TYPE = 'SEDOL'
BEGIN
  INSERT #SECURITY (bdate, sedol) VALUES (@BDATE, @IDENTIFIER_VALUE)
  UPDATE #SECURITY SET sedol = equity_common.dbo.fnSedolIncludeCheckDigit(sedol)
END
ELSE IF @IDENTIFIER_TYPE = 'ISIN'
  BEGIN INSERT #SECURITY (bdate, isin) VALUES (@BDATE, @IDENTIFIER_VALUE) END

DECLARE
@SQL varchar(1000),
@SECURITY_ID int

SELECT @SQL = 'UPDATE #SECURITY '
SELECT @SQL = @SQL + 'SET security_id = y.security_id '
SELECT @SQL = @SQL + 'FROM strategy g, universe_makeup p, equity_common..security y '
SELECT @SQL = @SQL + 'WHERE g.strategy_id = '+CONVERT(varchar,@STRATEGY_ID)+' '
SELECT @SQL = @SQL + 'AND g.universe_id = p.universe_id '
SELECT @SQL = @SQL + 'AND p.universe_dt = '''+CONVERT(varchar,@BDATE,112)+''' '
SELECT @SQL = @SQL + 'AND p.security_id = y.security_id '
SELECT @SQL = @SQL + 'AND #SECURITY.'+@IDENTIFIER_TYPE+' = y.'+@IDENTIFIER_TYPE

IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
EXEC(@SQL)

IF @DEBUG = 1
BEGIN
  SELECT '#SECURITY (1)'
  SELECT * FROM #SECURITY
END

IF EXISTS (SELECT 1 FROM #SECURITY WHERE security_id IS NULL)
  BEGIN EXEC security_id_update @TABLE_NAME='#SECURITY' END

IF @DEBUG = 1
BEGIN
  SELECT '#SECURITY (2)'
  SELECT * FROM #SECURITY
END

IF NOT EXISTS (SELECT 1 FROM #SECURITY WHERE security_id IS NOT NULL)
  BEGIN RETURN 0 END

SELECT @SECURITY_ID = security_id FROM #SECURITY

DROP TABLE #SECURITY

CREATE TABLE #RESULT (
  security_id	int				NULL,
  ticker		varchar(32)		NULL,
  cusip			varchar(32)		NULL,
  sedol			varchar(32)		NULL,
  isin			varchar(32)		NULL,
  imnt_nm		varchar(100)	NULL,

  region_id		int				NULL,
  region_nm		varchar(128)	NULL,
  country_cd	varchar(50)		NULL,
  country_nm	varchar(128)	NULL,
  sector_id		int				NULL,
  sector_nm		varchar(64)		NULL,
  segment_id	int				NULL,
  segment_nm	varchar(128)	NULL,

  account_wgt		float		NULL,
  benchmark_wgt		float		NULL,
  model_wgt			float		NULL,

  acct_bm_wgt		float		NULL,
  mpf_bm_wgt		float		NULL,
  acct_mpf_wgt		float		NULL
)

IF EXISTS (SELECT 1 FROM equity_common..position
            WHERE reference_date = @BDATE
              AND reference_date = effective_date
              AND acct_cd IN (SELECT acct_cd FROM equity_common..account WHERE parent = @ACCOUNT_CD
                              UNION
                              SELECT acct_cd FROM equity_common..account WHERE acct_cd = @ACCOUNT_CD)
              AND security_id = @SECURITY_ID)
BEGIN
  CREATE TABLE #POS (
    security_id	int		NULL,
    units		float	NULL,
    price		float	NULL,
    mval		float	NULL
  )

  INSERT #POS (security_id, units, price)
  SELECT security_id, 0.0, 0.0
    FROM equity_common..position
   WHERE reference_date = @BDATE
     AND reference_date = effective_date
     AND acct_cd IN (SELECT acct_cd FROM equity_common..account WHERE parent = @ACCOUNT_CD
                     UNION
                     SELECT acct_cd FROM equity_common..account WHERE acct_cd = @ACCOUNT_CD)
     AND security_id IS NOT NULL

  UPDATE #POS
     SET units = x.quantity
    FROM (SELECT security_id, SUM(ISNULL(quantity,0.0)) AS [quantity]
            FROM equity_common..position
           WHERE reference_date = @BDATE
             AND reference_date = effective_date
             AND acct_cd IN (SELECT acct_cd FROM equity_common..account WHERE parent = @ACCOUNT_CD
                             UNION
                             SELECT acct_cd FROM equity_common..account WHERE acct_cd = @ACCOUNT_CD)
           GROUP BY security_id) x
   WHERE #POS.security_id = x.security_id

  UPDATE #POS
     SET price = ISNULL(p.price_close_usd,0.0)
    FROM equity_common..market_price p
   WHERE p.reference_date = @BDATE
     AND #POS.security_id = p.security_id

  UPDATE #POS
     SET mval = units * price

  IF @DEBUG = 1
  BEGIN
    SELECT '#POS'
    SELECT * FROM #POS ORDER BY security_id
  END

  DECLARE @ACCOUNT_MVAL float
  SELECT @ACCOUNT_MVAL = SUM(mval) FROM #POS

  IF @ACCOUNT_MVAL != 0.0
  BEGIN
    INSERT #RESULT (security_id, account_wgt, benchmark_wgt, model_wgt)
    SELECT security_id, mval / @ACCOUNT_MVAL, 0.0, 0.0
      FROM #POS
     WHERE security_id = @SECURITY_ID
  END
  ELSE
  BEGIN
    INSERT #RESULT (security_id, account_wgt, benchmark_wgt, model_wgt)
    SELECT @SECURITY_ID, 0.0, 0.0, 0.0
  END

  DROP TABLE #POS
END
ELSE
BEGIN
  INSERT #RESULT (security_id, account_wgt, benchmark_wgt, model_wgt)
  SELECT @SECURITY_ID, 0.0, 0.0, 0.0
END

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (1)'
  SELECT * FROM #RESULT
END

UPDATE #RESULT
   SET ticker = y.ticker,
       cusip = y.cusip,
       sedol = y.sedol,
       isin = y.isin,
       imnt_nm = y.security_name,
       country_cd = ISNULL(y.domicile_iso_cd, y.issue_country_cd)
  FROM equity_common..security y
 WHERE y.security_id = @SECURITY_ID

UPDATE #RESULT
   SET country_nm = UPPER(c.country_name)
  FROM equity_common..country c
 WHERE #RESULT.country_cd = c.country_cd

UPDATE #RESULT
   SET region_id = d.region_id,
       region_nm = d.region_nm
  FROM strategy g, region_def d, region_makeup p
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.region_model_id = d.region_model_id
   AND d.region_id = p.region_id
   AND #RESULT.country_cd = p.country_cd

UPDATE #RESULT
   SET sector_id = ss.sector_id,
       segment_id = ss.segment_id
  FROM sector_model_security ss, strategy g, factor_model f
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = f.factor_model_id
   AND ss.bdate = @BDATE
   AND ss.sector_model_id = f.sector_model_id
   AND ss.security_id = @SECURITY_ID

UPDATE #RESULT
   SET sector_nm = d.sector_nm
  FROM sector_def d
 WHERE #RESULT.sector_id = d.sector_id

UPDATE #RESULT
   SET segment_nm = d.segment_nm
  FROM segment_def d
 WHERE #RESULT.segment_id = d.segment_id

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (2)'
  SELECT * FROM #RESULT
END

IF EXISTS (SELECT 1 FROM account a, benchmark b
            WHERE a.strategy_id = @STRATEGY_ID
              AND a.account_cd = @ACCOUNT_CD
              AND a.benchmark_cd = b.benchmark_cd)
BEGIN
  UPDATE #RESULT
     SET benchmark_wgt = w.weight
    FROM account a, equity_common..benchmark_weight w
   WHERE a.strategy_id = @STRATEGY_ID
     AND a.account_cd = @ACCOUNT_CD
     AND a.benchmark_cd = w.acct_cd
     AND w.reference_date = @BDATE
     AND w.reference_date = w.effective_date
     AND w.security_id = @SECURITY_ID
END
ELSE
BEGIN
  UPDATE #RESULT
     SET benchmark_wgt = p.weight / 100.0
    FROM account a, universe_def d, universe_makeup p
   WHERE a.strategy_id = @STRATEGY_ID
     AND a.account_cd = @ACCOUNT_CD
     AND a.benchmark_cd = d.universe_cd
     AND d.universe_id = p.universe_id
     AND p.universe_dt = @BDATE
     AND p.security_id = @SECURITY_ID
END

DECLARE @MODEL_ID int
IF @MODEL_WEIGHT='CAP'
BEGIN
  SELECT @MODEL_ID = d2.universe_id
    FROM strategy g, universe_def d1, universe_def d2
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.universe_id = d1.universe_id
     AND d2.universe_cd = d1.universe_cd + '_MPF_CAP'
END
ELSE IF @MODEL_WEIGHT='EQUAL'
BEGIN
  SELECT @MODEL_ID = d2.universe_id
    FROM strategy g, universe_def d1, universe_def d2
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.universe_id = d1.universe_id
     AND d2.universe_cd = d1.universe_cd + '_MPF_EQL'
END

UPDATE #RESULT
   SET model_wgt = m.weight / 100.0
  FROM universe_makeup m
 WHERE m.universe_dt = @BDATE
   AND m.universe_id = @MODEL_ID
   AND m.security_id = @SECURITY_ID

IF EXISTS (SELECT 1 FROM decode WHERE item='MODEL WEIGHT NULL' AND code=@STRATEGY_ID)
  BEGIN UPDATE #RESULT SET model_wgt = NULL END

UPDATE #RESULT
   SET acct_bm_wgt = account_wgt - benchmark_wgt,
       mpf_bm_wgt = model_wgt - benchmark_wgt,
       acct_mpf_wgt = account_wgt - model_wgt

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (3)'
  SELECT * FROM #RESULT
END
--0
SELECT ticker		AS [Ticker],
       cusip		AS [CUSIP],
       sedol		AS [SEDOL],
       isin			AS [ISIN],
       imnt_nm		AS [Name],
       region_nm	AS [Region Name],
       country_nm	AS [Country Name],
       ISNULL(sector_nm, 'UNKNOWN') AS [Sector Name],
       ISNULL(segment_nm, 'UNKNOWN') AS [Segment Name],
       account_wgt	AS [Account],
       benchmark_wgt AS [Benchmark],
       model_wgt	AS [Model],
       acct_bm_wgt	AS [Acct-Bmk],
       mpf_bm_wgt	AS [Model-Bmk],
       acct_mpf_wgt	AS [Acct-Model]
  FROM #RESULT

IF NOT EXISTS (SELECT 1 FROM scores WHERE bdate = @BDATE
                                      AND strategy_id = @STRATEGY_ID
                                      AND security_id = @SECURITY_ID)
  BEGIN RETURN 0 END

CREATE TABLE #SCORE (
  score_type	varchar(64)	NULL,
  score_weight	float		NULL,
  score_value	float		NULL
)

INSERT #SCORE
SELECT 'TOTAL SCORE', NULL, s.total_score
  FROM #RESULT r LEFT JOIN scores s ON s.security_id = r.security_id AND s.bdate = @BDATE AND s.strategy_id = @STRATEGY_ID

INSERT #SCORE
SELECT 'UNIVERSE SCORE', w.universe_total_wgt, s.universe_score
  FROM #RESULT r LEFT JOIN scores s ON s.security_id = r.security_id AND s.bdate = @BDATE AND s.strategy_id = @STRATEGY_ID
 INNER JOIN factor_model_weights w ON ISNULL(r.sector_id,-9999) = ISNULL(w.sector_id,-9999) AND ISNULL(r.segment_id,-9999) = ISNULL(w.segment_id,-9999)
 INNER JOIN strategy g ON g.factor_model_id = w.factor_model_id AND g.strategy_id = @STRATEGY_ID
 WHERE w.universe_total_wgt != 0.0
INSERT #SCORE --SEGMENT MAY EXIST SUCH AS WITH STANDARD GICS SECTOR MODEL BUT NOT UTILIZED
SELECT 'UNIVERSE SCORE', w.universe_total_wgt, s.universe_score
  FROM #RESULT r LEFT JOIN scores s ON s.security_id = r.security_id AND s.bdate = @BDATE AND s.strategy_id = @STRATEGY_ID
 INNER JOIN factor_model_weights w ON ISNULL(r.sector_id,-9999) = ISNULL(w.sector_id,-9999)
 INNER JOIN strategy g ON g.factor_model_id = w.factor_model_id AND g.strategy_id = @STRATEGY_ID
 WHERE w.universe_total_wgt != 0.0
   AND NOT EXISTS (SELECT 1 FROM #SCORE WHERE score_type = 'UNIVERSE SCORE')

INSERT #SCORE
SELECT 'SECTOR SCORE', w.sector_ss_wgt * w.ss_total_wgt, s.sector_score
  FROM #RESULT r LEFT JOIN scores s ON s.security_id = r.security_id AND s.bdate = @BDATE AND s.strategy_id = @STRATEGY_ID
 INNER JOIN factor_model_weights w ON ISNULL(r.sector_id,-9999) = ISNULL(w.sector_id,-9999) AND ISNULL(r.segment_id,-9999) = ISNULL(w.segment_id,-9999)
 INNER JOIN strategy g ON g.factor_model_id = w.factor_model_id AND g.strategy_id = @STRATEGY_ID
 WHERE w.sector_ss_wgt != 0.0
INSERT #SCORE --SAME AS PREVIOUS COMMENT
SELECT 'SECTOR SCORE', w.sector_ss_wgt * w.ss_total_wgt, s.sector_score
  FROM #RESULT r LEFT JOIN scores s ON s.security_id = r.security_id AND s.bdate = @BDATE AND s.strategy_id = @STRATEGY_ID
 INNER JOIN factor_model_weights w ON ISNULL(r.sector_id,-9999) = ISNULL(w.sector_id,-9999)
 INNER JOIN strategy g ON g.factor_model_id = w.factor_model_id AND g.strategy_id = @STRATEGY_ID
 WHERE w.sector_ss_wgt != 0.0
   AND NOT EXISTS (SELECT 1 FROM #SCORE WHERE score_type = 'SECTOR SCORE')

INSERT #SCORE
SELECT 'SEGMENT SCORE', w.segment_ss_wgt * w.ss_total_wgt, s.segment_score
  FROM #RESULT r LEFT JOIN scores s ON s.security_id = r.security_id AND s.bdate = @BDATE AND s.strategy_id = @STRATEGY_ID
 INNER JOIN factor_model_weights w ON r.sector_id = w.sector_id AND r.segment_id = w.segment_id
 INNER JOIN strategy g ON g.factor_model_id = w.factor_model_id AND g.strategy_id = @STRATEGY_ID
 WHERE w.segment_ss_wgt != 0.0

INSERT #SCORE
SELECT 'REGION SCORE', w.region_total_wgt, s.region_score
  FROM #RESULT r LEFT JOIN scores s ON s.security_id = r.security_id AND s.bdate = @BDATE AND s.strategy_id = @STRATEGY_ID
 INNER JOIN factor_model_weights w ON ISNULL(r.sector_id,-9999) = ISNULL(w.sector_id,-9999) AND ISNULL(r.segment_id,-9999) = ISNULL(w.segment_id,-9999)
 INNER JOIN strategy g ON g.factor_model_id = w.factor_model_id AND g.strategy_id = @STRATEGY_ID
 WHERE w.region_total_wgt != 0.0
INSERT #SCORE --SAME AS PREVIOUS COMMENT
SELECT 'REGION SCORE', w.region_total_wgt, s.region_score
  FROM #RESULT r LEFT JOIN scores s ON s.security_id = r.security_id AND s.bdate = @BDATE AND s.strategy_id = @STRATEGY_ID
 INNER JOIN factor_model_weights w ON ISNULL(r.sector_id,-9999) = ISNULL(w.sector_id,-9999)
 INNER JOIN strategy g ON g.factor_model_id = w.factor_model_id AND g.strategy_id = @STRATEGY_ID
 WHERE w.region_total_wgt != 0.0
   AND NOT EXISTS (SELECT 1 FROM #SCORE WHERE score_type = 'REGION SCORE')

INSERT #SCORE
SELECT 'COUNTRY SCORE', w.country_total_wgt, s.country_score
  FROM #RESULT r LEFT JOIN scores s ON s.security_id = r.security_id AND s.bdate = @BDATE AND s.strategy_id = @STRATEGY_ID
 INNER JOIN factor_model_weights w ON ISNULL(r.sector_id,-9999) = ISNULL(w.sector_id,-9999) AND ISNULL(r.segment_id,-9999) = ISNULL(w.segment_id,-9999)
 INNER JOIN strategy g ON g.factor_model_id = w.factor_model_id AND g.strategy_id = @STRATEGY_ID
 WHERE w.country_total_wgt != 0.0
INSERT #SCORE --SAME AS PREVIOUS COMMENT
SELECT 'COUNTRY SCORE', w.country_total_wgt, s.country_score
  FROM #RESULT r LEFT JOIN scores s ON s.security_id = r.security_id AND s.bdate = @BDATE AND s.strategy_id = @STRATEGY_ID
 INNER JOIN factor_model_weights w ON ISNULL(r.sector_id,-9999) = ISNULL(w.sector_id,-9999)
 INNER JOIN strategy g ON g.factor_model_id = w.factor_model_id AND g.strategy_id = @STRATEGY_ID
 WHERE w.country_total_wgt != 0.0
   AND NOT EXISTS (SELECT 1 FROM #SCORE WHERE score_type = 'COUNTRY SCORE')

IF @DEBUG = 1
BEGIN
  SELECT '#SCORE'
  SELECT * FROM #SCORE
END
--1
IF EXISTS (SELECT 1 FROM #SCORE WHERE score_type = 'TOTAL SCORE' AND score_value IS NULL)
  BEGIN SELECT 'TOTAL SCORE' AS [Score Type], NULL AS [Score Wgt], NULL AS [Score Value] END
ELSE
BEGIN
  SELECT score_type AS [Score Type], score_weight AS [Score Wgt], ROUND(score_value,1) AS [Score Value]
    FROM #SCORE WHERE score_type = 'TOTAL SCORE'
END
--2
IF EXISTS (SELECT 1 FROM #SCORE WHERE score_type = 'UNIVERSE SCORE' AND score_value IS NULL)
  BEGIN SELECT 'UNIVERSE SCORE' AS [Score Type], NULL AS [Score Wgt], NULL AS [Score Value] END
ELSE
BEGIN
  SELECT score_type	 AS [Score Type], score_weight AS [Score Wgt], ROUND(score_value,1) AS [Score Value]
    FROM #SCORE WHERE score_type = 'UNIVERSE SCORE'
END
--3
IF EXISTS (SELECT 1 FROM #SCORE WHERE score_type = 'SECTOR SCORE' AND score_value IS NULL)
  BEGIN SELECT 'SECTOR SCORE' AS [Score Type], NULL AS [Score Wgt], NULL AS [Score Value] END
ELSE
BEGIN
  SELECT score_type AS [Score Type], score_weight AS [Score Wgt], ROUND(score_value,1) AS [Score Value]
    FROM #SCORE WHERE score_type = 'SECTOR SCORE'
END
--4
IF EXISTS (SELECT 1 FROM #SCORE WHERE score_type = 'SEGMENT SCORE' AND score_value IS NULL)
  BEGIN SELECT 'SEGMENT SCORE' AS [Score Type], NULL AS [Score Wgt], NULL AS [Score Value] END
ELSE
BEGIN
  SELECT score_type AS [Score Type], score_weight AS [Score Wgt], ROUND(score_value,1) AS [Score Value]
    FROM #SCORE WHERE score_type = 'SEGMENT SCORE'
END
--5
IF EXISTS (SELECT 1 FROM #SCORE WHERE score_type = 'REGION SCORE' AND score_value IS NULL)
  BEGIN SELECT 'REGION SCORE' AS [Score Type], NULL AS [Score Wgt], NULL AS [Score Value] END
ELSE
BEGIN
  SELECT score_type AS [Score Type], score_weight AS [Score Wgt], ROUND(score_value,1) AS [Score Value]
    FROM #SCORE WHERE score_type = 'REGION SCORE'
END
--6
IF EXISTS (SELECT 1 FROM #SCORE WHERE score_type = 'COUNTRY SCORE' AND score_value IS NULL)
  BEGIN SELECT 'COUNTRY SCORE' AS [Score Type], NULL AS [Score Wgt], NULL AS [Score Value] END
ELSE
BEGIN
  SELECT score_type AS [Score Type], score_weight AS [Score Wgt], ROUND(score_value,1) AS [Score Value]
    FROM #SCORE WHERE score_type = 'COUNTRY SCORE'
END

DROP TABLE #SCORE

CREATE TABLE #RANK (
  rank_event_id	int			NULL,
  against		varchar(1)	NULL,
  category		varchar(1)	NULL,

  factor_id			int				NULL,
  factor_cd			varchar(32)		NULL,
  factor_short_nm	varchar(64)		NULL,
  factor_nm			varchar(255)	NULL,

  weight	float	NULL,
  rank		int		NULL
)

INSERT #RANK (rank_event_id, against, factor_id, weight, rank)
SELECT i.rank_event_id, i.against, i.factor_id, w.weight, o.rank
  FROM strategy g, factor_against_weight w, rank_inputs i, rank_output o
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = w.factor_model_id
   AND i.bdate = @BDATE
   AND i.universe_id = g.universe_id
   AND i.factor_id = w.factor_id
   AND i.against = w.against
   AND ISNULL(i.against_id,-9999) = ISNULL(w.against_id,-9999)
   AND i.rank_event_id = o.rank_event_id
   AND o.security_id = @SECURITY_ID

IF @DEBUG = 1
BEGIN
  SELECT '#RANK (1)'
  SELECT * FROM #RANK ORDER BY against, factor_id
END

--OVERRIDE WEIGHT LOGIC: BEGIN
IF EXISTS (SELECT 1 FROM strategy g, factor_against_weight_override o
            WHERE g.strategy_id = @STRATEGY_ID AND g.factor_model_id = o.factor_model_id)
BEGIN
  UPDATE #RANK
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RANK.factor_id = o.factor_id
     AND #RANK.against = o.against
     AND #RANK.against = 'U'
     AND o.level_type = 'G'
     AND o.level_id = r.segment_id
  UPDATE #RANK
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RANK.factor_id = o.factor_id
     AND #RANK.against = o.against
     AND #RANK.against = 'R'
     AND ISNULL(r.region_id, -9999) = ISNULL(o.against_id, -9999)
     AND o.level_type = 'G'
     AND o.level_id = r.segment_id
  UPDATE #RANK
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RANK.factor_id = o.factor_id
     AND #RANK.against = o.against
     AND #RANK.against = 'C'
     AND ISNULL(r.sector_id, -9999) = ISNULL(o.against_id, -9999)
     AND o.level_type = 'G'
     AND o.level_id = r.segment_id
  UPDATE #RANK
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RANK.factor_id = o.factor_id
     AND #RANK.against = o.against
     AND #RANK.against = 'G'
     AND ISNULL(r.segment_id, -9999) = ISNULL(o.against_id, -9999)
     AND o.level_type = 'G'
     AND o.level_id = r.segment_id

  UPDATE #RANK
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RANK.factor_id = o.factor_id
     AND #RANK.against = o.against
     AND #RANK.against = 'U'
     AND o.level_type = 'C'
     AND o.level_id = r.sector_id
  UPDATE #RANK
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RANK.factor_id = o.factor_id
     AND #RANK.against = o.against
     AND #RANK.against = 'R'
     AND ISNULL(r.region_id, -9999) = ISNULL(o.against_id, -9999)
     AND o.level_type = 'C'
     AND o.level_id = r.sector_id
  UPDATE #RANK
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RANK.factor_id = o.factor_id
     AND #RANK.against = o.against
     AND #RANK.against = 'C'
     AND ISNULL(r.sector_id, -9999) = ISNULL(o.against_id, -9999)
     AND o.level_type = 'C'
     AND o.level_id = r.sector_id
  UPDATE #RANK
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RANK.factor_id = o.factor_id
     AND #RANK.against = o.against
     AND #RANK.against = 'G'
     AND ISNULL(r.segment_id, -9999) = ISNULL(o.against_id, -9999)
     AND o.level_type = 'C'
     AND o.level_id = r.sector_id

  UPDATE #RANK
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RANK.factor_id = o.factor_id
     AND #RANK.against = o.against
     AND #RANK.against = 'U'
     AND o.level_type = 'U'
  UPDATE #RANK
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RANK.factor_id = o.factor_id
     AND #RANK.against = 'R'
     AND ISNULL(r.region_id, -9999) = ISNULL(o.against_id, -9999)
     AND o.level_type = 'U'
  UPDATE #RANK
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RANK.factor_id = o.factor_id
     AND #RANK.against = 'C'
     AND ISNULL(r.sector_id, -9999) = ISNULL(o.against_id, -9999)
     AND o.level_type = 'U'
  UPDATE #RANK
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RANK.factor_id = o.factor_id
     AND #RANK.against = o.against
     AND #RANK.against = 'G'
     AND ISNULL(r.segment_id, -9999) = ISNULL(o.against_id, -9999)
     AND o.level_type = 'U'
END
--OVERRIDE WEIGHT LOGIC: END

/*
NOTE: CODE FOR WEIGHT OVERRIDES INVOLVING COUNTRY AND REGION IS INCOMPLETE;
      FOR COUNTRY, WOULD REQUIRE ADDING COLUMN level_cd TO TABLE factor_against_weight_override
*/

IF @DEBUG = 1
BEGIN
  SELECT '#RANK (2)'
  SELECT * FROM #RANK ORDER BY against, factor_id
END

DROP TABLE #RESULT

DELETE #RANK WHERE weight = 0.0

UPDATE #RANK
   SET factor_cd = f.factor_cd,
       factor_short_nm = f.factor_short_nm,
       factor_nm = f.factor_nm
  FROM factor f
 WHERE #RANK.factor_id = f.factor_id

UPDATE #RANK
   SET category = c.category
  FROM strategy g, factor_category c
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = c.factor_model_id
   AND #RANK.factor_id = c.factor_id

IF @DEBUG = 1
BEGIN
  SELECT '#RANK (3)'
  SELECT * FROM #RANK ORDER BY against, factor_id
END

CREATE TABLE #CATEGORY (
  ordinal		int identity(1,1)	NOT NULL,
  category_cd	varchar(1)		NOT NULL,
  category_nm	varchar(64)		NOT NULL
)

INSERT #CATEGORY (category_cd, category_nm)
SELECT code, decode
  FROM decode
 WHERE item = 'FACTOR_CATEGORY'
   AND code IN (SELECT DISTINCT category FROM #RANK)
 ORDER BY decode

IF @DEBUG = 1
BEGIN
  SELECT '#CATEGORY'
  SELECT * FROM #CATEGORY ORDER BY ordinal
END

CREATE TABLE #SCORE_LEVEL (
  ordinal		int identity(1,1)	NOT NULL,
  score_lvl_cd	varchar(1)		NOT NULL,
  score_lvl_nm	varchar(32)		NOT NULL
)

INSERT #SCORE_LEVEL (score_lvl_cd, score_lvl_nm) SELECT code, decode FROM decode WHERE item = 'SCORE_LEVEL' AND code = 'U'
INSERT #SCORE_LEVEL (score_lvl_cd, score_lvl_nm) SELECT code, decode FROM decode WHERE item = 'SCORE_LEVEL' AND code = 'C'
INSERT #SCORE_LEVEL (score_lvl_cd, score_lvl_nm) SELECT code, decode FROM decode WHERE item = 'SCORE_LEVEL' AND code = 'G'
INSERT #SCORE_LEVEL (score_lvl_cd, score_lvl_nm) SELECT code, decode FROM decode WHERE item = 'SCORE_LEVEL' AND code = 'R'
INSERT #SCORE_LEVEL (score_lvl_cd, score_lvl_nm) SELECT code, decode FROM decode WHERE item = 'SCORE_LEVEL' AND code = 'Y'

IF @DEBUG = 1
BEGIN
  SELECT '#SCORE_LEVEL'
  SELECT * FROM #SCORE_LEVEL ORDER BY ordinal
END
--6.5
SELECT c.category_nm AS [category],
       l.score_lvl_nm AS [score_level],
       s.category_score AS [category_score]
  FROM #CATEGORY c CROSS JOIN #SCORE_LEVEL l LEFT JOIN category_score s
    ON c.category_cd = s.category
   AND l.score_lvl_cd = s.score_level
   AND s.bdate = @BDATE
   AND s.strategy_id = @STRATEGY_ID
   AND s.security_id = @SECURITY_ID
 ORDER BY c.category_nm, l.ordinal

DROP TABLE #SCORE_LEVEL

DECLARE @NUM int,
        @CATEGORY varchar(64)

SELECT @NUM=0
WHILE EXISTS (SELECT * FROM #CATEGORY WHERE ordinal > @NUM)
BEGIN
  SELECT @NUM = MIN(ordinal) FROM #CATEGORY WHERE ordinal > @NUM
  SELECT @CATEGORY = category_cd FROM #CATEGORY WHERE ordinal = @NUM

  SELECT @SQL = 'SELECT r.factor_short_nm AS [Factor], r.factor_nm AS [Factor Name], r.weight AS [Weight], r.rank AS [Rank] '
  SELECT @SQL = @SQL + 'FROM #RANK r, decode d WHERE r.against = ''U'' AND d.item = ''FACTOR_CATEGORY'' AND r.category = d.code '
  SELECT @SQL = @SQL + 'AND d.code = ''' + @CATEGORY + ''' ORDER BY r.against, d.decode, r.factor_nm'
  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)

  SELECT @SQL = 'SELECT r.factor_short_nm AS [Factor], r.factor_nm AS [Factor Name], r.weight AS [Weight], r.rank AS [Rank] '
  SELECT @SQL = @SQL + 'FROM #RANK r, decode d WHERE r.against = ''C'' AND d.item = ''FACTOR_CATEGORY'' AND r.category = d.code '
  SELECT @SQL = @SQL + 'AND d.code = ''' + @CATEGORY + ''' ORDER BY r.against, d.decode, r.factor_nm'
  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)

  SELECT @SQL = 'SELECT r.factor_short_nm AS [Factor], r.factor_nm AS [Factor Name], r.weight AS [Weight], r.rank AS [Rank] '
  SELECT @SQL = @SQL + 'FROM #RANK r, decode d WHERE r.against = ''G'' AND d.item = ''FACTOR_CATEGORY'' AND r.category = d.code '
  SELECT @SQL = @SQL + 'AND d.code = ''' + @CATEGORY + ''' ORDER BY r.against, d.decode, r.factor_nm'
  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)

  SELECT @SQL = 'SELECT r.factor_short_nm AS [Factor], r.factor_nm AS [Factor Name], r.weight AS [Weight], r.rank AS [Rank] '
  SELECT @SQL = @SQL + 'FROM #RANK r, decode d WHERE r.against = ''R'' AND d.item = ''FACTOR_CATEGORY'' AND r.category = d.code '
  SELECT @SQL = @SQL + 'AND d.code = ''' + @CATEGORY + ''' ORDER BY r.against, d.decode, r.factor_nm'
  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)

  SELECT @SQL = 'SELECT r.factor_short_nm AS [Factor], r.factor_nm AS [Factor Name], r.weight AS [Weight], r.rank AS [Rank] '
  SELECT @SQL = @SQL + 'FROM #RANK r, decode d WHERE r.against = ''Y'' AND d.item = ''FACTOR_CATEGORY'' AND r.category = d.code '
  SELECT @SQL = @SQL + 'AND d.code = ''' + @CATEGORY + ''' ORDER BY r.against, d.decode, r.factor_nm'
  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)
END

DROP TABLE #CATEGORY
DROP TABLE #RANK

RETURN 0
go
IF OBJECT_ID('dbo.rpt_stock_view_current') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_stock_view_current >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_stock_view_current >>>'
go
