use QER
go
IF OBJECT_ID('dbo.rpt_changes_to_model') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rpt_changes_to_model
    IF OBJECT_ID('dbo.rpt_changes_to_model') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rpt_changes_to_model >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rpt_changes_to_model >>>'
END
go
CREATE PROCEDURE dbo.rpt_changes_to_model
@BDATE datetime,
@STRATEGY_ID int,
@ACCOUNT_CD varchar(32),
@PERIODS int,
@PERIOD_TYPE varchar(2),
@REPORT_VIEW varchar(16),
@SCORE_CHANGE_MIN int = NULL,
@DEBUG bit = NULL
AS
/* MODEL - MOVERS */

/****
* KNOWN ISSUES:
*   THIS PROCEDURE WILL NOT HANDLE SITUATION WHERE
*   - THE FACTOR MODEL OR SECTOR MODEL HAS CHANGED BETWEEN THE TWO DATES;
*     DATABASE CONTAINS LATEST MODELS ONLY (I.E. LOOKS AT PREV_BDATE WITH CURRENT MODELS)
*   - SECURITY HAS CHANGED CLASSIFICATION IN RUSSELL AND/OR GICS SECTOR MODEL(S)
****/

IF @BDATE IS NULL
  BEGIN SELECT 'ERROR: @BDATE IS A REQUIRED PARAMETER' RETURN -1 END
IF @STRATEGY_ID IS NULL
  BEGIN SELECT 'ERROR: @STRATEGY_ID IS A REQUIRED PARAMETER' RETURN -1 END
IF @ACCOUNT_CD IS NULL
  BEGIN SELECT 'ERROR: @ACCOUNT_CD IS A REQUIRED PARAMETER' RETURN -1 END
IF @PERIODS IS NULL
  BEGIN SELECT 'ERROR: @PERIODS IS A REQUIRED PARAMETER' RETURN -1 END
IF @PERIODS > 0
  BEGIN SELECT @PERIODS = -1 * @PERIODS END
IF @PERIOD_TYPE IS NULL
  BEGIN SELECT 'ERROR: @PERIOD_TYPE IS A REQUIRED PARAMETER' RETURN -1 END
IF @PERIOD_TYPE NOT IN ('YY', 'YYYY', 'QQ', 'Q', 'MM', 'M', 'WK', 'WW', 'DD', 'D')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @PERIOD_TYPE PARAMETER' RETURN -1 END
IF @REPORT_VIEW IS NULL
  BEGIN SELECT 'ERROR: @REPORT_VIEW IS A REQUIRED PARAMETER' RETURN -1 END
IF @REPORT_VIEW NOT IN ('RANKS', 'MODEL', 'UNIVERSE')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSESD FOR @REPORT_VIEW PARAMETER' RETURN -1 END

DECLARE
@ADATE datetime,
@PREV_BDATE datetime,
@UNIVERSE_ID int

EXEC business_date_get @DIFF=0, @REF_DATE=@BDATE, @RET_DATE=@ADATE OUTPUT

IF @ADATE != @BDATE
BEGIN
  SELECT @ADATE = @BDATE
  EXEC business_date_get @DIFF=-1, @REF_DATE=@BDATE, @RET_DATE=@BDATE OUTPUT
END

IF @PERIOD_TYPE IN ('YY','YYYY')
  BEGIN SELECT @PREV_BDATE = DATEADD(YY, @PERIODS, @ADATE) END
ELSE IF @PERIOD_TYPE IN ('QQ','Q')
  BEGIN SELECT @PREV_BDATE = DATEADD(QQ, @PERIODS, @ADATE) END
ELSE IF @PERIOD_TYPE IN ('MM','M')
  BEGIN SELECT @PREV_BDATE = DATEADD(MM, @PERIODS, @ADATE) END
ELSE IF @PERIOD_TYPE IN ('WK','WW')
  BEGIN SELECT @PREV_BDATE = DATEADD(WK, @PERIODS, @ADATE) END
ELSE IF @PERIOD_TYPE IN ('DD','D')
  BEGIN SELECT @PREV_BDATE = DATEADD(DD, @PERIODS, @ADATE) END

IF @DEBUG = 1
  BEGIN SELECT '@PREV_BDATE', @PREV_BDATE END

EXEC business_date_get @DIFF=0, @REF_DATE=@PREV_BDATE, @RET_DATE=@ADATE OUTPUT

IF @ADATE != @PREV_BDATE
  BEGIN EXEC business_date_get @DIFF=-1, @REF_DATE=@PREV_BDATE, @RET_DATE=@PREV_BDATE OUTPUT END

IF @DEBUG = 1
  BEGIN SELECT '@PREV_BDATE', @PREV_BDATE END

IF @PREV_BDATE < '20070101'
BEGIN
  DROP TABLE #POSITION
  RETURN 0
END

SELECT @PREV_BDATE AS [From], @BDATE AS [To]

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
  acct_bmk_wgt		float		NULL,

  curr_total_score		float	NULL,
  prev_total_score		float	NULL,
  change_total_score	float	NULL
)

IF @REPORT_VIEW = 'RANKS'
BEGIN
  INSERT #RESULT (security_id, account_wgt, benchmark_wgt, acct_bmk_wgt, curr_total_score, prev_total_score, change_total_score)
  SELECT s2.security_id, 0.0, 0.0, 0.0, s2.total_score, s1.total_score, s2.total_score - s1.total_score
    FROM scores s1, scores s2
   WHERE s1.bdate = @PREV_BDATE
     AND s1.strategy_id = @STRATEGY_ID
     AND s1.total_score IS NOT NULL
     AND s2.bdate = @BDATE
     AND s2.strategy_id = @STRATEGY_ID
     AND s2.total_score IS NOT NULL
     AND s1.security_id = s2.security_id

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT: RANKS (1)'
    SELECT * FROM #RESULT ORDER BY security_id
  END

  IF @SCORE_CHANGE_MIN IS NULL
  BEGIN
    SELECT @SCORE_CHANGE_MIN = fractile / 5
      FROM strategy
     WHERE strategy_id = @STRATEGY_ID
  END

  DELETE #RESULT WHERE ABS(change_total_score) < @SCORE_CHANGE_MIN

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT: RANKS (2)'
    SELECT * FROM #RESULT ORDER BY security_id
  END
END
ELSE IF @REPORT_VIEW IN ('MODEL', 'UNIVERSE')
BEGIN
  IF @REPORT_VIEW = 'MODEL'
  BEGIN
    SELECT @UNIVERSE_ID = d2.universe_id
      FROM strategy g, universe_def d1, universe_def d2
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.universe_id = d1.universe_id
       AND d2.universe_cd = d1.universe_cd + '_MPF_CAP' --DOES NOT MATTER EQUAL OR CAP WEIGHTED, JUST NEED CONSTITUENTS OF MODEL PORTFOLIO
  END
  ELSE IF @REPORT_VIEW = 'UNIVERSE'
  BEGIN
    SELECT @UNIVERSE_ID = universe_id
      FROM strategy
     WHERE strategy_id = @STRATEGY_ID
  END

  IF @DEBUG = 1
    BEGIN SELECT '@UNIVERSE_ID', @UNIVERSE_ID END

  INSERT #RESULT (security_id, account_wgt, benchmark_wgt, acct_bmk_wgt)
  SELECT ISNULL(p1.security_id, p2.security_id), 0.0, 0.0, 0.0
    FROM (SELECT security_id FROM universe_makeup WHERE universe_dt = @PREV_BDATE AND universe_id = @UNIVERSE_ID) p1
          FULL OUTER JOIN 
         (SELECT security_id FROM universe_makeup WHERE universe_dt = @BDATE AND universe_id = @UNIVERSE_ID) p2
          ON p1.security_id = p2.security_id
   WHERE p1.security_id IS NULL
      OR p2.security_id IS NULL

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT: MODEL OR UNIVERSE (1)'
    SELECT * FROM #RESULT ORDER BY security_id
  END

  UPDATE #RESULT
     SET curr_total_score = s.total_score
    FROM scores s
   WHERE s.bdate = @BDATE
     AND s.strategy_id = @STRATEGY_ID
     AND #RESULT.security_id = s.security_id

  UPDATE #RESULT
     SET prev_total_score = s.total_score
    FROM scores s
   WHERE s.bdate = @PREV_BDATE
     AND s.strategy_id = @STRATEGY_ID
     AND #RESULT.security_id = s.security_id

  UPDATE #RESULT
     SET change_total_score = curr_total_score - prev_total_score

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT: MODEL OR UNIVERSE (2)'
    SELECT * FROM #RESULT ORDER BY security_id
  END
END

UPDATE #RESULT
   SET ticker = y.ticker,
       cusip = y.cusip,
       sedol = y.sedol,
       isin = y.isin,
       imnt_nm = y.security_name,
       country_cd = ISNULL(y.domicile_iso_cd, y.issue_country_cd)
  FROM equity_common..security y
 WHERE #RESULT.security_id = y.security_id

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
   AND #RESULT.security_id = ss.security_id

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
  SELECT '#RESULT (3)'
  SELECT * FROM #RESULT ORDER BY security_id
END

CREATE TABLE #POS (
  security_id	int		NULL,
  units			float	NULL,
  price			float	NULL,
  mval			float	NULL
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
  UPDATE #RESULT
     SET account_wgt = p.mval / @ACCOUNT_MVAL
    FROM #POS p
   WHERE #RESULT.security_id = p.security_id
END

DROP TABLE #POS

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
     AND #RESULT.security_id = w.security_id
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
     AND #RESULT.security_id = p.security_id
END

UPDATE #RESULT
   SET acct_bmk_wgt = account_wgt - benchmark_wgt

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (4)'
  SELECT * FROM #RESULT ORDER BY security_id
END

DECLARE @SQL varchar(1500),
        @SQL2 varchar(1500)

SELECT @SQL = 'SELECT ticker AS [Ticker], '
SELECT @SQL = @SQL + 'cusip AS [CUSIP], '
SELECT @SQL = @SQL + 'sedol AS [SEDOL], '
SELECT @SQL = @SQL + 'isin AS [ISIN], '
SELECT @SQL = @SQL + 'imnt_nm AS [Name], '
SELECT @SQL = @SQL + 'region_nm AS [Region Name], '
SELECT @SQL = @SQL + 'country_nm AS [Country Name], '
SELECT @SQL = @SQL + 'ISNULL(sector_nm, ''UNKNOWN'') AS [Sector Name], '
SELECT @SQL = @SQL + 'ISNULL(segment_nm, ''UNKNOWN'') AS [Segment Name], '
SELECT @SQL = @SQL + 'acct_bmk_wgt AS [Acct-Bmk Wgt], '
SELECT @SQL = @SQL + 'curr_total_score AS [Current], '
SELECT @SQL = @SQL + 'prev_total_score AS [Previous], '
SELECT @SQL = @SQL + 'change_total_score AS [Change] '
SELECT @SQL = @SQL + 'FROM #RESULT '
SELECT @SQL2 = @SQL

IF @REPORT_VIEW = 'RANKS'
BEGIN
  IF EXISTS (SELECT * FROM strategy WHERE strategy_id = @STRATEGY_ID AND rank_order = 1)
  BEGIN
    SELECT @SQL = @SQL + 'WHERE change_total_score >= 0.0 '
    SELECT @SQL = @SQL + 'ORDER BY change_total_score DESC, cusip, sedol, ticker, isin'

    SELECT @SQL2 = @SQL2 + 'WHERE change_total_score < 0.0 '
    SELECT @SQL2 = @SQL2 + 'ORDER BY change_total_score, cusip, sedol, ticker, isin'
  END
  ELSE
  BEGIN
    SELECT @SQL = @SQL + 'WHERE change_total_score < 0.0 '
    SELECT @SQL = @SQL + 'ORDER BY change_total_score, cusip, sedol, ticker, isin'

    SELECT @SQL2 = @SQL2 + 'WHERE change_total_score >= 0.0 '
    SELECT @SQL2 = @SQL2 + 'ORDER BY change_total_score DESC, cusip, sedol, ticker, isin'
  END
END
ELSE IF @REPORT_VIEW = 'MODEL'
BEGIN
  IF EXISTS (SELECT * FROM strategy WHERE strategy_id = @STRATEGY_ID AND rank_order = 1)
  BEGIN
    SELECT @SQL = @SQL + 'WHERE curr_total_score IS NOT NULL '
    SELECT @SQL = @SQL + 'AND ISNULL(change_total_score, 9999) >= 0.0 '
    SELECT @SQL = @SQL + 'ORDER BY curr_total_score DESC, change_total_score DESC, cusip, sedol, ticker, isin'

    SELECT @SQL2 = @SQL2 + 'WHERE prev_total_score IS NOT NULL '
    SELECT @SQL2 = @SQL2 + 'AND ISNULL(change_total_score, -9999) < 0.0 '
    SELECT @SQL2 = @SQL2 + 'ORDER BY prev_total_score DESC, change_total_score, cusip, sedol, ticker, isin'
  END
  ELSE
  BEGIN
    SELECT @SQL = @SQL + 'WHERE curr_total_score IS NOT NULL '
    SELECT @SQL = @SQL + 'AND ISNULL(change_total_score, -9999) < 0.0 '
    SELECT @SQL = @SQL + 'ORDER BY curr_total_score, change_total_score, cusip, sedol, ticker, isin'

    SELECT @SQL2 = @SQL2 + 'WHERE prev_total_score IS NOT NULL '
    SELECT @SQL2 = @SQL2 + 'AND ISNULL(change_total_score, 9999) >= 0.0 '
    SELECT @SQL2 = @SQL2 + 'ORDER BY prev_total_score, change_total_score DESC, cusip, sedol, ticker, isin'
  END
END
ELSE IF @REPORT_VIEW = 'UNIVERSE'
BEGIN
  SELECT @SQL = @SQL + 'WHERE curr_total_score IS NOT NULL '
  SELECT @SQL2 = @SQL2 + 'WHERE prev_total_score IS NOT NULL '

  IF EXISTS (SELECT * FROM strategy WHERE strategy_id = @STRATEGY_ID AND rank_order = 1)
  BEGIN
     SELECT @SQL = @SQL + 'ORDER BY curr_total_score DESC, cusip, sedol, ticker, isin'
     SELECT @SQL2 = @SQL2 + 'ORDER BY prev_total_score DESC, cusip, sedol, ticker, isin'
  END
  ELSE
  BEGIN
     SELECT @SQL = @SQL + 'ORDER BY curr_total_score, cusip, sedol, ticker, isin'
     SELECT @SQL2 = @SQL2 + 'ORDER BY prev_total_score, cusip, sedol, ticker, isin'
  END
END

IF @DEBUG = 1
BEGIN
  SELECT '@SQL', @SQL
  SELECT '@SQL2', @SQL2
END

EXEC(@SQL)
EXEC(@SQL2)

IF @REPORT_VIEW IN ('RANKS', 'MODEL')
BEGIN
  CREATE TABLE #RESULT2 (
    security_id		int		NULL,

    factor_id		int				NULL,
    factor_cd		varchar(32)		NULL,
    factor_short_nm	varchar(64)		NULL,
    factor_nm		varchar(255)	NULL,

    against			varchar(1)	NULL,
    against_cd		varchar(8)	NULL,
    against_id		int			NULL,
    weight1			float		NULL,
    weight2			float		NULL,

    curr_rank		int		NULL,
    prev_rank		int		NULL,
    change_rank		int		NULL
  )

  SELECT @UNIVERSE_ID = universe_id FROM strategy WHERE strategy_id = @STRATEGY_ID

  IF @DEBUG = 1
    BEGIN SELECT '@UNIVERSE_ID', @UNIVERSE_ID END

  INSERT #RESULT2 (security_id, factor_id, against, against_cd, against_id, weight1, curr_rank, prev_rank)
  SELECT x.security_id, x.factor_id, x.against, x.against_cd, x.against_id, 0.0, x.rank, y.rank
    FROM (SELECT o.security_id, i.factor_id, i.against, i.against_cd, i.against_id, o.rank
            FROM #RESULT r, rank_inputs i, rank_output o
           WHERE i.bdate = @BDATE
             AND i.universe_id = @UNIVERSE_ID
             AND i.rank_event_id = o.rank_event_id
             AND r.security_id = o.security_id) x
          INNER JOIN
         (SELECT o.security_id, i.factor_id, i.against, i.against_cd, i.against_id, o.rank
            FROM #RESULT r, rank_inputs i, rank_output o
           WHERE i.bdate = @PREV_BDATE
             AND i.universe_id = @UNIVERSE_ID
             AND i.rank_event_id = o.rank_event_id
             AND r.security_id = o.security_id) y
          ON x.security_id = y.security_id
         AND x.factor_id = y.factor_id
         AND x.against = y.against
         AND ISNULL(x.against_cd, 'NULLJOIN') = ISNULL(y.against_cd, 'NULLJOIN')
         AND ISNULL(x.against_id, -9999) = ISNULL(y.against_id, -9999)

  UPDATE #RESULT2 SET change_rank = curr_rank - prev_rank

  UPDATE #RESULT2
     SET factor_cd = f.factor_cd,
         factor_short_nm = f.factor_short_nm,
         factor_nm = f.factor_nm
    FROM factor f
   WHERE #RESULT2.factor_id = f.factor_id

  UPDATE #RESULT2
     SET weight1 = w.weight
    FROM strategy g, factor_against_weight w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND #RESULT2.factor_id = w.factor_id
     AND #RESULT2.against = w.against
     AND ISNULL(#RESULT2.against_id, -9999) = ISNULL(w.against_id, -9999)

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT2 (1)'
    SELECT * FROM #RESULT2 ORDER BY security_id, against, against_cd, against_id, factor_id
  END

  --OVERRIDE WEIGHT LOGIC: BEGIN
  IF EXISTS (SELECT 1 FROM strategy g, factor_against_weight_override o
              WHERE g.strategy_id = @STRATEGY_ID AND g.factor_model_id = o.factor_model_id)
  BEGIN
    UPDATE #RESULT2
       SET weight1 = o.override_wgt
      FROM #RESULT r, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RESULT2.factor_id = o.factor_id
       AND #RESULT2.against = o.against
       AND #RESULT2.against = 'U'
       AND o.level_type = 'G'
       AND o.level_id = r.segment_id
       AND #RESULT2.security_id = r.security_id
    UPDATE #RESULT2
       SET weight1 = o.override_wgt
      FROM #RESULT r, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RESULT2.factor_id = o.factor_id
       AND #RESULT2.against = o.against
       AND #RESULT2.against = 'R'
       AND ISNULL(r.region_id, -9999) = ISNULL(o.against_id, -9999)
       AND o.level_type = 'G'
       AND o.level_id = r.segment_id
       AND #RESULT2.security_id = r.security_id
    UPDATE #RESULT2
       SET weight1 = o.override_wgt
      FROM #RESULT r, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RESULT2.factor_id = o.factor_id
       AND #RESULT2.against = o.against
       AND #RESULT2.against = 'C'
       AND ISNULL(r.sector_id, -9999) = ISNULL(o.against_id, -9999)
       AND o.level_type = 'G'
       AND o.level_id = r.segment_id
       AND #RESULT2.security_id = r.security_id
    UPDATE #RESULT2
       SET weight1 = o.override_wgt
      FROM #RESULT r, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RESULT2.factor_id = o.factor_id
       AND #RESULT2.against = o.against
       AND #RESULT2.against = 'G'
       AND ISNULL(r.segment_id, -9999) = ISNULL(o.against_id, -9999)
       AND o.level_type = 'G'
       AND o.level_id = r.segment_id
       AND #RESULT2.security_id = r.security_id

    UPDATE #RESULT2
       SET weight1 = o.override_wgt
      FROM #RESULT r, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RESULT2.factor_id = o.factor_id
       AND #RESULT2.against = o.against
       AND #RESULT2.against = 'U'
       AND o.level_type = 'C'
       AND o.level_id = r.sector_id
       AND #RESULT2.security_id = r.security_id
    UPDATE #RESULT2
       SET weight1 = o.override_wgt
      FROM #RESULT r, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RESULT2.factor_id = o.factor_id
       AND #RESULT2.against = o.against
       AND #RESULT2.against = 'R'
       AND ISNULL(r.region_id, -9999) = ISNULL(o.against_id, -9999)
       AND o.level_type = 'C'
       AND o.level_id = r.sector_id
       AND #RESULT2.security_id = r.security_id
    UPDATE #RESULT2
       SET weight1 = o.override_wgt
      FROM #RESULT r, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RESULT2.factor_id = o.factor_id
       AND #RESULT2.against = o.against
       AND #RESULT2.against = 'C'
       AND ISNULL(r.sector_id, -9999) = ISNULL(o.against_id, -9999)
       AND o.level_type = 'C'
       AND o.level_id = r.sector_id
       AND #RESULT2.security_id = r.security_id
    UPDATE #RESULT2
       SET weight1 = o.override_wgt
      FROM #RESULT r, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RESULT2.factor_id = o.factor_id
       AND #RESULT2.against = o.against
       AND #RESULT2.against = 'G'
       AND ISNULL(r.segment_id, -9999) = ISNULL(o.against_id, -9999)
       AND o.level_type = 'C'
       AND o.level_id = r.sector_id
       AND #RESULT2.security_id = r.security_id

    UPDATE #RESULT2
       SET weight1 = o.override_wgt
      FROM #RESULT r, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RESULT2.factor_id = o.factor_id
       AND #RESULT2.against = o.against
       AND #RESULT2.against = 'U'
       AND o.level_type = 'U'
       AND #RESULT2.security_id = r.security_id
    UPDATE #RESULT2
       SET weight1 = o.override_wgt
      FROM #RESULT r, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RESULT2.factor_id = o.factor_id
       AND #RESULT2.against = o.against
       AND #RESULT2.against = 'R'
       AND ISNULL(r.region_id, -9999) = ISNULL(o.against_id, -9999)
       AND o.level_type = 'U'
       AND #RESULT2.security_id = r.security_id
    UPDATE #RESULT2
       SET weight1 = o.override_wgt
      FROM #RESULT r, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RESULT2.factor_id = o.factor_id
       AND #RESULT2.against = o.against
       AND #RESULT2.against = 'C'
       AND ISNULL(r.sector_id, -9999) = ISNULL(o.against_id, -9999)
       AND o.level_type = 'U'
       AND #RESULT2.security_id = r.security_id
    UPDATE #RESULT2
       SET weight1 = o.override_wgt
      FROM #RESULT r, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RESULT2.factor_id = o.factor_id
       AND #RESULT2.against = o.against
       AND #RESULT2.against = 'G'
       AND ISNULL(r.segment_id, -9999) = ISNULL(o.against_id, -9999)
       AND o.level_type = 'U'
       AND #RESULT2.security_id = r.security_id
  END
  --OVERRIDE WEIGHT LOGIC: END

  /*
  NOTE: CODE FOR WEIGHT OVERRIDES INVOLVING COUNTRY AND REGION IS INCOMPLETE;
        FOR COUNTRY, WOULD REQUIRE ADDING COLUMN level_cd TO TABLE factor_against_weight_override
  */

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT2 (2)'
    SELECT * FROM #RESULT2 ORDER BY security_id, against, against_cd, against_id, factor_id
  END

  DELETE #RESULT2 WHERE weight1 = 0.0

  UPDATE #RESULT2
     SET weight2 = weight1 * w.segment_ss_wgt * w.ss_total_wgt
    FROM #RESULT r, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND #RESULT2.security_id = r.security_id
     AND ISNULL(r.sector_id, -9999) = ISNULL(w.sector_id, -9999)
     AND ISNULL(r.segment_id, -9999) = ISNULL(w.segment_id, -9999)
     AND #RESULT2.against = 'G'
  UPDATE #RESULT2
     SET weight2 = weight1 * w.segment_ss_wgt * w.ss_total_wgt
    FROM #RESULT r, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND #RESULT2.security_id = r.security_id
     AND ISNULL(r.sector_id, -9999) = ISNULL(w.sector_id, -9999) --SEGMENT MAY EXIST SUCH AS WITH STANDARD GICS SECTOR MODEL BUT NOT UTILIZED
     AND #RESULT2.against = 'G'
     AND #RESULT2.weight2 IS NULL

  UPDATE #RESULT2
     SET weight2 = weight1 * w.sector_ss_wgt * w.ss_total_wgt
    FROM #RESULT r, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND #RESULT2.security_id = r.security_id
     AND ISNULL(r.sector_id, -9999) = ISNULL(w.sector_id, -9999)
     AND ISNULL(r.segment_id, -9999) = ISNULL(w.segment_id, -9999)
     AND #RESULT2.against = 'C'
  UPDATE #RESULT2
     SET weight2 = weight1 * w.sector_ss_wgt * w.ss_total_wgt
    FROM #RESULT r, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND #RESULT2.security_id = r.security_id
     AND ISNULL(r.sector_id, -9999) = ISNULL(w.sector_id, -9999) --SAME AS ABOVE
     AND #RESULT2.against = 'C'
     AND #RESULT2.weight2 IS NULL

  UPDATE #RESULT2
     SET weight2 = weight1 * w.universe_total_wgt
    FROM #RESULT r, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND #RESULT2.security_id = r.security_id
     AND ISNULL(r.sector_id, -9999) = ISNULL(w.sector_id, -9999)
     AND ISNULL(r.segment_id, -9999) = ISNULL(w.segment_id, -9999)
     AND #RESULT2.against = 'U'
  UPDATE #RESULT2
     SET weight2 = weight1 * w.universe_total_wgt
    FROM #RESULT r, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND #RESULT2.security_id = r.security_id
     AND ISNULL(r.sector_id, -9999) = ISNULL(w.sector_id, -9999) --SAME AS ABOVE
     AND #RESULT2.against = 'U'
     AND #RESULT2.weight2 IS NULL

  UPDATE #RESULT2
     SET weight2 = weight1 * w.country_total_wgt
    FROM #RESULT r, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND #RESULT2.security_id = r.security_id
     AND ISNULL(r.sector_id, -9999) = ISNULL(w.sector_id, -9999)
     AND ISNULL(r.segment_id, -9999) = ISNULL(w.segment_id, -9999)
     AND #RESULT2.against = 'Y'
  UPDATE #RESULT2
     SET weight2 = weight1 * w.country_total_wgt
    FROM #RESULT r, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND #RESULT2.security_id = r.security_id
     AND ISNULL(r.sector_id, -9999) = ISNULL(w.sector_id, -9999) --SAME AS ABOVE
     AND #RESULT2.against = 'Y'
     AND #RESULT2.weight2 IS NULL

  UPDATE #RESULT2
     SET weight2 = weight1 * w.region_total_wgt
    FROM #RESULT r, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND #RESULT2.security_id = r.security_id
     AND ISNULL(r.sector_id, -9999) = ISNULL(w.sector_id, -9999)
     AND ISNULL(r.segment_id, -9999) = ISNULL(w.segment_id, -9999)
     AND #RESULT2.against = 'R'
  UPDATE #RESULT2
     SET weight2 = weight1 * w.region_total_wgt
    FROM #RESULT r, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND #RESULT2.security_id = r.security_id
     AND ISNULL(r.sector_id, -9999) = ISNULL(w.sector_id, -9999) --SAME AS ABOVE
     AND #RESULT2.against = 'R'
     AND #RESULT2.weight2 IS NULL

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT2 (3)'
    SELECT * FROM #RESULT2 ORDER BY security_id, against, against_cd, against_id, factor_id
  END

  SELECT @SQL = 'SELECT r.ticker AS [Ticker], '
  SELECT @SQL = @SQL + 'r.cusip AS [CUSIP], '
  SELECT @SQL = @SQL + 'r.sedol AS [SEDOL], '
  SELECT @SQL = @SQL + 'r.isin AS [ISIN], '
  SELECT @SQL = @SQL + 'r2.factor_short_nm AS [Factor], '
  SELECT @SQL = @SQL + 'r2.factor_nm AS [Factor Name], '
  SELECT @SQL = @SQL + 'CASE r2.against WHEN ''U'' THEN ''UNIVERSE'' '
  SELECT @SQL = @SQL + 'WHEN ''R'' THEN ''REGION'' '
  SELECT @SQL = @SQL + 'WHEN ''Y'' THEN ''COUNTRY'' '
  SELECT @SQL = @SQL + 'WHEN ''C'' THEN ''SECTOR'' '
  SELECT @SQL = @SQL + 'WHEN ''G'' THEN ''SEGMENT'' END AS [Relative To], '
  SELECT @SQL = @SQL + 'r2.weight2 AS [Weight], '
  SELECT @SQL = @SQL + 'r2.curr_rank AS [Current Rank], '
  SELECT @SQL = @SQL + 'r2.prev_rank AS [Previous Rank], '
  SELECT @SQL = @SQL + 'r2.change_rank AS [Rank Change] '
  SELECT @SQL = @SQL + 'FROM #RESULT r, #RESULT2 r2 '
  SELECT @SQL = @SQL + 'WHERE r.security_id = r2.security_id '

  IF EXISTS (SELECT * FROM strategy WHERE strategy_id = @STRATEGY_ID AND rank_order = 1)
    BEGIN SELECT @SQL = @SQL + 'ORDER BY r.cusip, r.sedol, r.ticker, r.isin, r2.change_rank DESC, r2.curr_rank DESC' END
  ELSE
    BEGIN SELECT @SQL = @SQL + 'ORDER BY r.cusip, r.sedol, r.ticker, r.isin, r2.change_rank, r2.curr_rank' END

  IF @DEBUG = 1
    BEGIN SELECT '@SQL', @SQL END

  EXEC(@SQL)

  DROP TABLE #RESULT2
END

DROP TABLE #RESULT

RETURN 0
go
IF OBJECT_ID('dbo.rpt_changes_to_model') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_changes_to_model >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_changes_to_model >>>'
go
