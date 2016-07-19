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
CREATE PROCEDURE dbo.rpt_stock_view_current @STRATEGY_ID int,
                                            @BDATE datetime,
                                            @ACCOUNT_CD varchar(32),
                                            @MODEL_WEIGHT varchar(16),
                                            @IDENTIFIER_TYPE varchar(32),
                                            @IDENTIFIER_VALUE varchar(64),
                                            @DEBUG bit = NULL
AS
/* STOCK - CURRENT RANKS */

/****
* KNOWN ISSUES:
*   IN THE RARE CASE WHEN THE ACCOUNT HOLDS A SECURITY THAT IS NOT IN THE UNIVERSE, PROCEDURE WILL NOT RETURN ANY RESULTS
*   THIS HAPPENS IN A CASE SUCH AS TICKER 'SPY'
*   ALL SECURITIES IN ACCOUNTS THAT BELONG TO A STRATEGY ARE CAPTURED IN THE FACTSET SCREEN,
*   HOWEVER IT EXCLUDES SECURITIES SUCH AS SPIDERS
****/

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
IF @IDENTIFIER_TYPE NOT IN ('TICKER', 'CUSIP', 'SEDOL')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @IDENTIFIER_TYPE PARAMETER' RETURN -1 END
IF @IDENTIFIER_VALUE IS NULL
  BEGIN SELECT 'ERROR: @IDENTIFIER_VALUE IS A REQUIRED PARAMETER' RETURN -1 END

CREATE TABLE #RESULT (
  mqa_id		varchar(32)		NULL,
  ticker		varchar(16)		NULL,
  cusip			varchar(32)		NULL,
  sedol			varchar(32)		NULL,
  isin			varchar(64)		NULL,
  imnt_nm		varchar(255)	NULL,

  country_cd	varchar(4)		NULL,
  country_nm	varchar(128)	NULL,
  sector_id		int				NULL,
  sector_nm		varchar(64)		NULL,
  segment_id	int				NULL,
  segment_nm	varchar(128)	NULL,

  units			float			NULL,
  price			float			NULL,
  mval			float			NULL,
  
  account_wgt		float		NULL,
  benchmark_wgt		float		NULL,
  model_wgt			float		NULL,

  acct_bm_wgt		float		NULL,
  mpf_bm_wgt		float		NULL,
  acct_mpf_wgt		float		NULL
)

INSERT #RESULT (mqa_id, ticker, cusip, sedol, isin, units)
SELECT p.mqa_id, p.ticker, p.cusip, p.sedol, p.isin, ISNULL(p.units, 0.0)
  FROM position p
 WHERE p.bdate = @BDATE
   AND p.account_cd = @ACCOUNT_CD

IF @IDENTIFIER_TYPE = 'TICKER'
BEGIN
  IF NOT EXISTS (SELECT * FROM #RESULT WHERE ticker = @IDENTIFIER_VALUE)
  BEGIN
    INSERT #RESULT (mqa_id, ticker, cusip, sedol, isin, units)
    SELECT i.mqa_id, i.ticker, i.cusip, i.sedol, i.isin, 0.0
      FROM instrument_characteristics i
     WHERE i.bdate = @BDATE
       AND i.ticker = @IDENTIFIER_VALUE
  END
END
IF @IDENTIFIER_TYPE = 'CUSIP'
BEGIN
  IF NOT EXISTS (SELECT * FROM #RESULT WHERE cusip = @IDENTIFIER_VALUE)
  BEGIN
    INSERT #RESULT (mqa_id, ticker, cusip, sedol, isin, units)
    SELECT i.mqa_id, i.ticker, i.cusip, i.sedol, i.isin, 0.0
      FROM instrument_characteristics i
     WHERE i.bdate = @BDATE
       AND i.cusip = @IDENTIFIER_VALUE
  END
END
IF @IDENTIFIER_TYPE = 'SEDOL'
BEGIN
  IF NOT EXISTS (SELECT * FROM #RESULT WHERE sedol = @IDENTIFIER_VALUE)
  BEGIN
    INSERT #RESULT (mqa_id, ticker, cusip, sedol, isin, units)
    SELECT i.mqa_id, i.ticker, i.cusip, i.sedol, i.isin, 0.0
      FROM instrument_characteristics i
     WHERE i.bdate = @BDATE
       AND i.sedol = @IDENTIFIER_VALUE
  END
END

IF @IDENTIFIER_TYPE IN ('TICKER', 'CUSIP')
BEGIN
  UPDATE #RESULT
     SET imnt_nm = i.imnt_nm,
         country_cd = i.country,
         price = i.price_close
    FROM instrument_characteristics i
   WHERE i.bdate = @BDATE
     AND #RESULT.cusip = i.cusip

  UPDATE #RESULT
     SET country_nm = d.decode
    FROM decode d
   WHERE d.item = 'COUNTRY'
     AND #RESULT.country_cd = d.code
END
ELSE
BEGIN
  UPDATE #RESULT
     SET imnt_nm = i.imnt_nm,
         country_cd = i.country,
         price = i.price_close
    FROM instrument_characteristics i
   WHERE i.bdate = @BDATE
     AND #RESULT.sedol = i.sedol

  UPDATE #RESULT
     SET country_nm = d.decode
    FROM decode d
   WHERE d.item = 'COUNTRY'
     AND #RESULT.country_cd = d.code
END

UPDATE #RESULT
   SET imnt_nm = 'CASH',
       country_cd = 'US',
       country_nm = 'UNITED STATES',
       price = 1.0
 WHERE cusip = '_USD'

UPDATE #RESULT
   SET mval = units * price

DECLARE @ACCOUNT_MVAL float

SELECT @ACCOUNT_MVAL = SUM(mval)
  FROM #RESULT

IF @ACCOUNT_MVAL != 0.0
BEGIN
  UPDATE #RESULT
     SET account_wgt = mval / @ACCOUNT_MVAL
END
ELSE
  BEGIN UPDATE #RESULT SET account_wgt = 0.0 END

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT: AFTER INITIAL INSERT AND UPDATES'
  SELECT * FROM #RESULT ORDER BY cusip, sedol
END

IF @IDENTIFIER_TYPE = 'TICKER'
BEGIN
  DELETE #RESULT
   WHERE ISNULL(ticker, '') != @IDENTIFIER_VALUE
END
IF @IDENTIFIER_TYPE = 'CUSIP'
BEGIN
  DELETE #RESULT
   WHERE ISNULL(cusip, '') != @IDENTIFIER_VALUE
END
IF @IDENTIFIER_TYPE = 'SEDOL'
BEGIN
  DELETE #RESULT
   WHERE ISNULL(sedol, '') != @IDENTIFIER_VALUE
END

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT: AFTER DELETE'
  SELECT * FROM #RESULT ORDER BY cusip, sedol
END

IF @IDENTIFIER_TYPE IN ('TICKER', 'CUSIP')
BEGIN
  UPDATE #RESULT
     SET sector_id = ss.sector_id,
         segment_id = ss.segment_id
    FROM sector_model_security ss, strategy g, factor_model f
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = f.factor_model_id
     AND ss.bdate = @BDATE
     AND ss.sector_model_id = f.sector_model_id
     AND #RESULT.cusip = ss.cusip
END
ELSE
BEGIN
  UPDATE #RESULT
     SET sector_id = ss.sector_id,
         segment_id = ss.segment_id
    FROM sector_model_security ss, strategy g, factor_model f
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = f.factor_model_id
     AND ss.bdate = @BDATE
     AND ss.sector_model_id = f.sector_model_id
     AND #RESULT.sedol = ss.sedol
END

UPDATE #RESULT
   SET sector_nm = d.sector_nm
  FROM sector_def d
 WHERE #RESULT.sector_id = d.sector_id

UPDATE #RESULT
   SET sector_nm = 'UNKNOWN'
 WHERE sector_nm IS NULL

UPDATE #RESULT
   SET segment_nm = d.segment_nm
  FROM segment_def d
 WHERE #RESULT.segment_id = d.segment_id

UPDATE #RESULT
   SET segment_nm = 'UNKNOWN'
 WHERE segment_nm IS NULL

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

IF @IDENTIFIER_TYPE IN ('TICKER', 'CUSIP')
BEGIN
  UPDATE #RESULT
     SET benchmark_wgt = b.weight / 100.0
    FROM account a, universe_makeup b
   WHERE a.strategy_id = @STRATEGY_ID
     AND a.account_cd = @ACCOUNT_CD
     AND a.bm_universe_id = b.universe_id
     AND b.universe_dt = @BDATE
     AND #RESULT.cusip = b.cusip

  UPDATE #RESULT
     SET model_wgt = m.weight / 100.0
    FROM universe_makeup m
   WHERE m.universe_dt = @BDATE
     AND m.universe_id = @MODEL_ID
     AND #RESULT.cusip = m.cusip
END
ELSE
BEGIN
  UPDATE #RESULT
     SET benchmark_wgt = b.weight / 100.0
    FROM account a, universe_makeup b
   WHERE a.strategy_id = @STRATEGY_ID
     AND a.account_cd = @ACCOUNT_CD
     AND a.bm_universe_id = b.universe_id
     AND b.universe_dt = @BDATE
     AND #RESULT.sedol = b.sedol

  UPDATE #RESULT
     SET model_wgt = m.weight / 100.0
    FROM universe_makeup m
   WHERE m.universe_dt = @BDATE
     AND m.universe_id = @MODEL_ID
     AND #RESULT.sedol = m.sedol
END

UPDATE #RESULT
   SET benchmark_wgt = 0.0
 WHERE benchmark_wgt IS NULL

UPDATE #RESULT
   SET model_wgt = 0.0
 WHERE model_wgt IS NULL

IF EXISTS (SELECT * FROM decode WHERE item='MODEL WEIGHT NULL' AND code=@STRATEGY_ID)
  BEGIN UPDATE #RESULT SET model_wgt = NULL END

UPDATE #RESULT
   SET acct_bm_wgt = account_wgt - benchmark_wgt,
       mpf_bm_wgt = model_wgt - benchmark_wgt,
       acct_mpf_wgt = account_wgt - model_wgt

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT: FINAL STATE'
  SELECT * FROM #RESULT ORDER BY cusip, sedol
END
--0
SELECT ticker		AS [Ticker],
       cusip		AS [CUSIP],
       sedol		AS [SEDOL],
       isin			AS [ISIN],
       imnt_nm		AS [Name],
       country_nm	AS [Country Name],
       sector_nm	AS [Sector Name],
       segment_nm	AS [Segment Name],
       account_wgt	AS [Account],
       benchmark_wgt AS [Benchmark],
       model_wgt	AS [Model],
       acct_bm_wgt	AS [Acct-Bmk],
       mpf_bm_wgt	AS [Model-Bmk],
       acct_mpf_wgt	AS [Acct-Model]
  FROM #RESULT

CREATE TABLE #SCORE (
  score_type	varchar(64)	NULL,
  score_weight	float		NULL,
  score_value	float		NULL
)

IF @IDENTIFIER_TYPE IN ('TICKER', 'CUSIP')
BEGIN
  INSERT #SCORE
  SELECT 'TOTAL SCORE', NULL, total_score = s.total_score
    FROM #RESULT r
    LEFT JOIN scores s ON s.cusip = r.cusip AND s.bdate = @BDATE AND s.strategy_id = @STRATEGY_ID

  INSERT #SCORE
  SELECT 'UNIVERSE SCORE', w.universe_total_wgt, universe_score = s.universe_score
    FROM #RESULT r LEFT JOIN scores s ON s.cusip = r.cusip AND s.bdate = @BDATE AND s.strategy_id = @STRATEGY_ID
   INNER JOIN factor_model_weights w ON r.sector_id = w.sector_id AND ISNULL(r.segment_id, 0) = ISNULL(w.segment_id, 0)
   INNER JOIN strategy g ON g.factor_model_id = w.factor_model_id AND g.strategy_id = @STRATEGY_ID

  INSERT #SCORE
  SELECT 'SECTOR SCORE', w.sector_ss_wgt * w.ss_total_wgt, sector_score = s.sector_score
    FROM #RESULT r LEFT JOIN scores s ON s.cusip = r.cusip AND s.bdate = @BDATE AND s.strategy_id = @STRATEGY_ID
   INNER JOIN factor_model_weights w ON r.sector_id = w.sector_id AND ISNULL(r.segment_id, 0) = ISNULL(w.segment_id, 0)
   INNER JOIN strategy g ON g.factor_model_id = w.factor_model_id AND g.strategy_id = @STRATEGY_ID
   WHERE w.sector_ss_wgt != 0.0

  INSERT #SCORE
  SELECT 'SEGMENT SCORE', w.segment_ss_wgt * w.ss_total_wgt, segment_score = s.segment_score
    FROM #RESULT r LEFT JOIN scores s ON s.cusip = r.cusip AND s.bdate = @BDATE AND s.strategy_id = @STRATEGY_ID
   INNER JOIN factor_model_weights w ON r.sector_id = w.sector_id AND r.segment_id = w.segment_id
   INNER JOIN strategy g ON g.factor_model_id = w.factor_model_id AND g.strategy_id = @STRATEGY_ID
   WHERE w.segment_ss_wgt != 0.0
END
ELSE
BEGIN
  INSERT #SCORE
  SELECT 'TOTAL SCORE', NULL, total_score = s.total_score
    FROM #RESULT r LEFT JOIN scores s ON s.sedol = r.sedol AND s.bdate = @BDATE AND s.strategy_id = @STRATEGY_ID

  INSERT #SCORE
  SELECT 'UNIVERSE SCORE', w.universe_total_wgt, universe_score = s.universe_score
    FROM #RESULT r LEFT JOIN scores s ON s.sedol = r.sedol AND s.bdate = @BDATE AND s.strategy_id = @STRATEGY_ID
   INNER JOIN factor_model_weights w ON r.sector_id = w.sector_id AND ISNULL(r.segment_id, 0) = ISNULL(w.segment_id, 0)
   INNER JOIN strategy g ON g.factor_model_id = w.factor_model_id AND g.strategy_id = @STRATEGY_ID

  INSERT #SCORE
  SELECT 'SECTOR SCORE', w.sector_ss_wgt * w.ss_total_wgt, sector_score = s.sector_score
    FROM #RESULT r LEFT JOIN scores s ON s.sedol = r.sedol AND s.bdate = @BDATE AND s.strategy_id = @STRATEGY_ID
   INNER JOIN factor_model_weights w ON r.sector_id = w.sector_id AND ISNULL(r.segment_id, 0) = ISNULL(w.segment_id, 0)
   INNER JOIN strategy g ON g.factor_model_id = w.factor_model_id AND g.strategy_id = @STRATEGY_ID
   WHERE w.sector_ss_wgt != 0.0

  INSERT #SCORE
  SELECT 'SEGMENT SCORE', w.segment_ss_wgt * w.ss_total_wgt, segment_score = s.segment_score
    FROM #RESULT r LEFT JOIN scores s ON s.sedol = r.sedol AND s.bdate = @BDATE AND s.strategy_id = @STRATEGY_ID
   INNER JOIN factor_model_weights w ON r.sector_id = w.sector_id AND r.segment_id = w.segment_id
   INNER JOIN strategy g ON g.factor_model_id = w.factor_model_id AND g.strategy_id = @STRATEGY_ID
   WHERE w.segment_ss_wgt != 0.0
END

IF @DEBUG = 1
BEGIN
  SELECT '#SCORE: AFTER INITIAL INSERTS'
  SELECT * FROM #SCORE ORDER BY score_type
END

DELETE #SCORE
 WHERE score_weight <= 0.0

IF @DEBUG = 1
BEGIN
  SELECT '#SCORE: FINAL STATE, AFTER DELETE'
  SELECT * FROM #SCORE ORDER BY score_type
END
--1
IF EXISTS (SELECT * FROM #SCORE WHERE score_type = 'TOTAL SCORE')
BEGIN
  SELECT score_type		AS [Score Type],
         score_weight	AS [Score Wgt],
         ROUND(score_value,1)	AS [Score Value]
    FROM #SCORE
   WHERE score_type = 'TOTAL SCORE'
END
ELSE
  BEGIN SELECT 'TOTAL SCORE' AS [Score Type], NULL AS [Score Wgt], NULL AS [Score Value] END
--2
IF EXISTS (SELECT * FROM #SCORE WHERE score_type = 'UNIVERSE SCORE' AND score_value IS NULL)
  BEGIN SELECT 'UNIVERSE SCORE' AS [Score Type], NULL AS [Score Wgt], NULL AS [Score Value] END
ELSE
BEGIN
  SELECT score_type		AS [Score Type],
         score_weight	AS [Score Wgt],
         ROUND(score_value,1)	AS [Score Value]
    FROM #SCORE
   WHERE score_type = 'UNIVERSE SCORE'
END
--3
IF EXISTS (SELECT * FROM #SCORE WHERE score_type = 'SECTOR SCORE' AND score_value IS NULL)
  BEGIN SELECT 'SECTOR SCORE' AS [Score Type], NULL AS [Score Wgt], NULL AS [Score Value] END
ELSE
BEGIN
  SELECT score_type		AS [Score Type],
         score_weight	AS [Score Wgt],
         ROUND(score_value,1)	AS [Score Value]
    FROM #SCORE
   WHERE score_type = 'SECTOR SCORE'
END
--4
IF EXISTS (SELECT * FROM #SCORE WHERE score_type = 'SEGMENT SCORE' AND score_value IS NULL)
  BEGIN SELECT 'SEGMENT SCORE' AS [Score Type], NULL AS [Score Wgt], NULL AS [Score Value] END
ELSE
BEGIN
  SELECT score_type		AS [Score Type],
         score_weight	AS [Score Wgt],
         ROUND(score_value,1)	AS [Score Value]
    FROM #SCORE
   WHERE score_type = 'SEGMENT SCORE'
END

CREATE TABLE #FACTOR_RANK (
  rank_event_id		int			NULL,
  against			varchar(1)	NULL,
  category			varchar(1)	NULL,

  factor_id			int				NULL,
  factor_cd			varchar(32)		NULL,
  factor_short_nm	varchar(64)		NULL,
  factor_nm			varchar(255)	NULL,

  weight			float	NULL,
  rank				int		NULL
)

INSERT #FACTOR_RANK
      (against, category, factor_id, factor_cd, factor_short_nm, factor_nm, weight)
SELECT w.against, c.category, f.factor_id, f.factor_cd, f.factor_short_nm, f.factor_nm, w.weight
  FROM factor_against_weight w, factor_category c, factor f, strategy g
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = w.factor_model_id
   AND w.factor_model_id = c.factor_model_id
   AND w.factor_id = c.factor_id
   AND c.factor_id = f.factor_id
   AND w.against = 'U'

INSERT #FACTOR_RANK
      (against, category, factor_id, factor_cd, factor_short_nm, factor_nm, weight)
SELECT w.against, c.category, f.factor_id, f.factor_cd, f.factor_short_nm, f.factor_nm, w.weight
  FROM #RESULT r, factor_against_weight w, factor_category c, factor f, strategy g
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = w.factor_model_id
   AND w.factor_model_id = c.factor_model_id
   AND w.factor_id = c.factor_id
   AND c.factor_id = f.factor_id
   AND w.against = 'C'
   AND w.against_id = r.sector_id

INSERT #FACTOR_RANK
      (against, category, factor_id, factor_cd, factor_short_nm, factor_nm, weight)
SELECT w.against, c.category, f.factor_id, f.factor_cd, f.factor_short_nm, f.factor_nm, w.weight
  FROM #RESULT r, factor_against_weight w, factor_category c, factor f, strategy g
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = w.factor_model_id
   AND w.factor_model_id = c.factor_model_id
   AND w.factor_id = c.factor_id
   AND c.factor_id = f.factor_id
   AND w.against = 'G'
   AND w.against_id = r.segment_id

IF @DEBUG = 1
BEGIN
  SELECT '#FACTOR_RANK: AFTER INITIAL INSERT'
  SELECT * FROM #FACTOR_RANK ORDER BY against, factor_id
END

--OVERRIDE WEIGHT LOGIC: BEGIN
UPDATE #FACTOR_RANK
   SET weight = o.override_wgt
  FROM #RESULT r, strategy g, factor_against_weight_override o
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = o.factor_model_id
   AND #FACTOR_RANK.factor_id = o.factor_id
   AND #FACTOR_RANK.against = o.against
   AND #FACTOR_RANK.against = 'U'
   AND o.level_type = 'G'
   AND o.level_id = r.segment_id
UPDATE #FACTOR_RANK
   SET weight = o.override_wgt
  FROM #RESULT r, strategy g, factor_against_weight_override o
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = o.factor_model_id
   AND #FACTOR_RANK.factor_id = o.factor_id
   AND #FACTOR_RANK.against = o.against
   AND #FACTOR_RANK.against = 'C'
   AND (r.sector_id = o.against_id OR (r.sector_id IS NULL AND o.against_id IS NULL))
   AND o.level_type = 'G'
   AND o.level_id = r.segment_id
UPDATE #FACTOR_RANK
   SET weight = o.override_wgt
  FROM #RESULT r, strategy g, factor_against_weight_override o
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = o.factor_model_id
   AND #FACTOR_RANK.factor_id = o.factor_id
   AND #FACTOR_RANK.against = o.against
   AND #FACTOR_RANK.against = 'G'
   AND (r.segment_id = o.against_id OR (r.segment_id IS NULL AND o.against_id IS NULL))
   AND o.level_type = 'G'
   AND o.level_id = r.segment_id

UPDATE #FACTOR_RANK
   SET weight = o.override_wgt
  FROM #RESULT r, strategy g, factor_against_weight_override o
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = o.factor_model_id
   AND #FACTOR_RANK.factor_id = o.factor_id
   AND #FACTOR_RANK.against = o.against
   AND #FACTOR_RANK.against = 'U'
   AND o.level_type = 'C'
   AND o.level_id = r.sector_id
UPDATE #FACTOR_RANK
   SET weight = o.override_wgt
  FROM #RESULT r, strategy g, factor_against_weight_override o
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = o.factor_model_id
   AND #FACTOR_RANK.factor_id = o.factor_id
   AND #FACTOR_RANK.against = o.against
   AND #FACTOR_RANK.against = 'C'
   AND (r.sector_id = o.against_id OR (r.sector_id IS NULL AND o.against_id IS NULL))
   AND o.level_type = 'C'
   AND o.level_id = r.sector_id
UPDATE #FACTOR_RANK
   SET weight = o.override_wgt
  FROM #RESULT r, strategy g, factor_against_weight_override o
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = o.factor_model_id
   AND #FACTOR_RANK.factor_id = o.factor_id
   AND #FACTOR_RANK.against = o.against
   AND #FACTOR_RANK.against = 'G'
   AND (r.segment_id = o.against_id OR (r.segment_id IS NULL AND o.against_id IS NULL))
   AND o.level_type = 'C'
   AND o.level_id = r.sector_id

UPDATE #FACTOR_RANK
   SET weight = o.override_wgt
  FROM #RESULT r, strategy g, factor_against_weight_override o
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = o.factor_model_id
   AND #FACTOR_RANK.factor_id = o.factor_id
   AND #FACTOR_RANK.against = o.against
   AND #FACTOR_RANK.against = 'U'
   AND o.level_type = 'U'
UPDATE #FACTOR_RANK
   SET weight = o.override_wgt
  FROM #RESULT r, strategy g, factor_against_weight_override o
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = o.factor_model_id
   AND #FACTOR_RANK.factor_id = o.factor_id
   AND #FACTOR_RANK.against = 'C'
   AND (r.sector_id = o.against_id OR (r.sector_id IS NULL AND o.against_id IS NULL))
   AND o.level_type = 'U'
UPDATE #FACTOR_RANK
   SET weight = o.override_wgt
  FROM #RESULT r, strategy g, factor_against_weight_override o
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = o.factor_model_id
   AND #FACTOR_RANK.factor_id = o.factor_id
   AND #FACTOR_RANK.against = o.against
   AND #FACTOR_RANK.against = 'G'
   AND (r.segment_id = o.against_id OR (r.segment_id IS NULL AND o.against_id IS NULL))
   AND o.level_type = 'U'
--OVERRIDE WEIGHT LOGIC: END

IF @DEBUG = 1
BEGIN
  SELECT '#FACTOR_RANK: AFTER WEIGHT OVERRIDE UPDATE'
  SELECT * FROM #FACTOR_RANK ORDER BY against, factor_id
END

DELETE #FACTOR_RANK
 WHERE weight = 0.0

UPDATE #FACTOR_RANK
   SET rank_event_id = i.rank_event_id
  FROM rank_inputs i, strategy g
 WHERE i.bdate = @BDATE
   AND g.strategy_id = @STRATEGY_ID
   AND i.universe_id = g.universe_id
   AND i.factor_id = #FACTOR_RANK.factor_id
   AND i.against = #FACTOR_RANK.against
   AND #FACTOR_RANK.against = 'U'

UPDATE #FACTOR_RANK
   SET rank_event_id = i.rank_event_id
  FROM #RESULT r, rank_inputs i, strategy g
 WHERE i.bdate = @BDATE
   AND g.strategy_id = @STRATEGY_ID
   AND i.universe_id = g.universe_id
   AND i.factor_id = #FACTOR_RANK.factor_id
   AND i.against = #FACTOR_RANK.against
   AND #FACTOR_RANK.against = 'C'
   AND i.against_id = r.sector_id

UPDATE #FACTOR_RANK
   SET rank_event_id = i.rank_event_id
  FROM #RESULT r, rank_inputs i, strategy g
 WHERE i.bdate = @BDATE
   AND g.strategy_id = @STRATEGY_ID
   AND i.universe_id = g.universe_id
   AND i.factor_id = #FACTOR_RANK.factor_id
   AND i.against = #FACTOR_RANK.against
   AND #FACTOR_RANK.against = 'G'
   AND i.against_id = r.segment_id

IF @IDENTIFIER_TYPE IN ('TICKER', 'CUSIP')
BEGIN
  UPDATE #FACTOR_RANK
     SET rank = o.rank
    FROM #RESULT r, rank_output o
   WHERE o.rank_event_id = #FACTOR_RANK.rank_event_id
     AND o.cusip = r.cusip
END
ELSE
BEGIN
  UPDATE #FACTOR_RANK
     SET rank = o.rank
    FROM #RESULT r, rank_output o
   WHERE o.rank_event_id = #FACTOR_RANK.rank_event_id
     AND o.sedol = r.sedol
END

IF @DEBUG = 1
BEGIN
  SELECT '#FACTOR_RANK: AFTER ALL UPDATES'
  SELECT * FROM #FACTOR_RANK ORDER BY against, factor_id
END

DECLARE @PRECALC bit,
        @NUM int,
        @CATEGORY varchar(64),
        @SQL varchar(1000)

SELECT @PRECALC = 0

IF @IDENTIFIER_TYPE IN ('TICKER', 'CUSIP')
BEGIN
  IF EXISTS (SELECT * FROM #RESULT r, category_score c
              WHERE c.bdate = @BDATE
                AND c.strategy_id = @STRATEGY_ID
                AND c.cusip = r.cusip)
    BEGIN SELECT @PRECALC = 1 END
END
ELSE IF @IDENTIFIER_TYPE = 'SEDOL'
BEGIN
  IF EXISTS (SELECT * FROM #RESULT r, category_score c
              WHERE c.bdate = @BDATE
                AND c.strategy_id = @STRATEGY_ID
                AND c.sedol = r.sedol)
    BEGIN SELECT @PRECALC = 1 END
END

CREATE TABLE #FACTOR_CATEGORY (
  ordinal		int identity(1,1)	NOT NULL,
  category_cd	varchar(1)		NOT NULL,
  category_nm	varchar(64)		NOT NULL
)

INSERT #FACTOR_CATEGORY (category_cd, category_nm)
SELECT code, decode
  FROM decode
 WHERE item = 'FACTOR_CATEGORY'
   AND code IN (SELECT DISTINCT category FROM #FACTOR_RANK)
 ORDER BY decode

IF @DEBUG = 1
BEGIN
  SELECT '#FACTOR_CATEGORY'
  SELECT * FROM #FACTOR_CATEGORY ORDER BY ordinal
END

CREATE TABLE #SCORE_LEVEL (
  ordinal		int identity(1,1)	NOT NULL,
  score_lvl_cd	varchar(1)		NOT NULL,
  score_lvl_nm	varchar(32)		NOT NULL
)

INSERT #SCORE_LEVEL (score_lvl_cd, score_lvl_nm) SELECT code, decode FROM decode WHERE item = 'SCORE_LEVEL' AND code = 'U'
INSERT #SCORE_LEVEL (score_lvl_cd, score_lvl_nm) SELECT code, decode FROM decode WHERE item = 'SCORE_LEVEL' AND code = 'C'
INSERT #SCORE_LEVEL (score_lvl_cd, score_lvl_nm) SELECT code, decode FROM decode WHERE item = 'SCORE_LEVEL' AND code = 'G'

IF @DEBUG = 1
BEGIN
  SELECT '#SCORE_LEVEL'
  SELECT * FROM #SCORE_LEVEL ORDER BY ordinal
END

IF @PRECALC = 1
BEGIN
  CREATE TABLE #CATEGORY_TEMP (
    category_cd		varchar(1)	NOT NULL,
    category_nm		varchar(64)	NULL,
    ordinal			int 		NOT NULL,
    score_lvl_cd	varchar(1)	NOT NULL,
    score_lvl_nm	varchar(32)	NOT NULL,
    category_score	float		NULL
  )

  IF @IDENTIFIER_TYPE IN ('TICKER', 'CUSIP')
  BEGIN
    INSERT #CATEGORY_TEMP (category_cd, category_nm, ordinal, score_lvl_cd, score_lvl_nm)
    SELECT DISTINCT c.category, d.decode, s.ordinal, s.score_lvl_cd, s.score_lvl_nm
      FROM #SCORE_LEVEL s, #RESULT r, category_score c, decode d
     WHERE c.bdate = @BDATE
       AND c.strategy_id = @STRATEGY_ID
       AND c.cusip = r.cusip
       AND c.score_level IN ('U', 'C', 'G')
       AND d.item = 'FACTOR_CATEGORY'
       AND c.category = d.code

    UPDATE #CATEGORY_TEMP
       SET category_score = c.category_score
      FROM #RESULT r, category_score c
     WHERE c.bdate = @BDATE
       AND c.strategy_id = @STRATEGY_ID
       AND c.cusip = r.cusip
       AND c.score_level IN ('U', 'C', 'G')
       AND #CATEGORY_TEMP.category_cd = c.category
       AND #CATEGORY_TEMP.score_lvl_cd = c.score_level
  END
  ELSE IF @IDENTIFIER_TYPE = 'SEDOL'
  BEGIN
    INSERT #CATEGORY_TEMP (category_cd, category_nm, ordinal, score_lvl_cd, score_lvl_nm)
    SELECT DISTINCT c.category, d.decode, s.ordinal, s.score_lvl_cd, s.score_lvl_nm
      FROM #SCORE_LEVEL s, #RESULT r, category_score c, decode d
     WHERE c.bdate = @BDATE
       AND c.strategy_id = @STRATEGY_ID
       AND c.sedol = r.sedol
       AND c.score_level IN ('U', 'C', 'G')
       AND d.item = 'FACTOR_CATEGORY'
       AND c.category = d.code

    UPDATE #CATEGORY_TEMP
       SET category_score = c.category_score
      FROM #RESULT r, category_score c
     WHERE c.bdate = @BDATE
       AND c.strategy_id = @STRATEGY_ID
       AND c.sedol = r.sedol
       AND c.score_level IN ('U', 'C', 'G')
       AND #CATEGORY_TEMP.category_cd = c.category
       AND #CATEGORY_TEMP.score_lvl_cd = c.score_level
  END
  --4.5
  SELECT category_nm AS [category],
         score_lvl_nm AS [score_level],
         category_score
    FROM #CATEGORY_TEMP
   ORDER BY category_nm, ordinal

  DROP TABLE #CATEGORY_TEMP
END
ELSE
BEGIN
  --4.5
  SELECT c.category_nm AS [category],
         l.score_lvl_nm AS [score_level],
         NULL AS [category_score]
    FROM #FACTOR_CATEGORY c, #SCORE_LEVEL l
   ORDER BY c.category_nm, l.ordinal
END

DROP TABLE #SCORE_LEVEL

SELECT @NUM=0
WHILE EXISTS (SELECT * FROM #FACTOR_CATEGORY WHERE ordinal > @NUM)
BEGIN
  SELECT @NUM = MIN(ordinal)
    FROM #FACTOR_CATEGORY
   WHERE ordinal > @NUM

  SELECT @CATEGORY = category_cd
    FROM #FACTOR_CATEGORY
   WHERE ordinal = @NUM

  SELECT @SQL = 'SELECT fr.factor_short_nm AS [Factor], fr.factor_nm AS [Factor Name], fr.weight AS [Weight], ISNULL(fr.rank, 0) AS [Rank] '
  SELECT @SQL = @SQL + 'FROM #FACTOR_RANK fr, decode d  WHERE fr.against = ''U'' AND d.item = ''FACTOR_CATEGORY'' AND fr.category = d.code '
  SELECT @SQL = @SQL + 'AND d.code = ''' + @CATEGORY + ''' ORDER BY fr.against, d.decode, fr.factor_nm'
  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)

  SELECT @SQL = 'SELECT fr.factor_short_nm AS [Factor], fr.factor_nm AS [Factor Name], fr.weight AS [Weight], ISNULL(fr.rank, 0) AS [Rank] '
  SELECT @SQL = @SQL + 'FROM #FACTOR_RANK fr, decode d  WHERE fr.against = ''C'' AND d.item = ''FACTOR_CATEGORY'' AND fr.category = d.code '
  SELECT @SQL = @SQL + 'AND d.code = ''' + @CATEGORY + ''' ORDER BY fr.against, d.decode, fr.factor_nm'
  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)

  SELECT @SQL = 'SELECT fr.factor_short_nm AS [Factor], fr.factor_nm AS [Factor Name], fr.weight AS [Weight], ISNULL(fr.rank, 0) AS [Rank] '
  SELECT @SQL = @SQL + 'FROM #FACTOR_RANK fr, decode d  WHERE fr.against = ''G'' AND d.item = ''FACTOR_CATEGORY'' AND fr.category = d.code '
  SELECT @SQL = @SQL + 'AND d.code = ''' + @CATEGORY + ''' ORDER BY fr.against, d.decode, fr.factor_nm'
  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)
END

DROP TABLE #FACTOR_CATEGORY
DROP TABLE #FACTOR_RANK
DROP TABLE #SCORE
DROP TABLE #RESULT

RETURN 0
go
IF OBJECT_ID('dbo.rpt_stock_view_current') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_stock_view_current >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_stock_view_current >>>'
go
