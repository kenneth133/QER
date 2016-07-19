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
CREATE PROCEDURE dbo.rpt_changes_to_model @BDATE datetime,
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

DECLARE @ADATE datetime
EXEC business_date_get @DIFF=0, @REF_DATE=@BDATE, @RET_DATE=@ADATE OUTPUT

IF @ADATE != @BDATE
BEGIN
  SELECT @ADATE = @BDATE
  EXEC business_date_get @DIFF=-1, @REF_DATE=@BDATE, @RET_DATE=@BDATE OUTPUT
END

CREATE TABLE #POSITION (
  security_id	int		NULL,
  units			float	NULL,
  price			float	NULL,
  mval			float	NULL,
  weight		float	NULL,
  bmk_wgt		float	NULL
)

INSERT #POSITION (security_id, units)
SELECT security_id, SUM(ISNULL(quantity,0.0))
  FROM equity_common..position
 WHERE reference_date = @BDATE
   AND reference_date = effective_date
   AND acct_cd IN (SELECT DISTINCT acct_cd FROM equity_common..account WHERE parent = @ACCOUNT_CD OR acct_cd = @ACCOUNT_CD)
 GROUP BY security_id

DELETE #POSITION WHERE units = 0.0

UPDATE #POSITION
   SET price = p.price_close_usd
  FROM equity_common..market_price p
 WHERE p.reference_date = @BDATE
   AND #POSITION.security_id = p.security_id

UPDATE #POSITION
   SET price = 0.0
 WHERE price IS NULL

UPDATE #POSITION
   SET mval = units * price

UPDATE #POSITION
   SET weight = mval / x.tot_mval
  FROM (SELECT SUM(mval) AS tot_mval FROM #POSITION) x

IF EXISTS (SELECT 1 FROM account a, benchmark b
            WHERE a.strategy_id = @STRATEGY_ID
              AND a.account_cd = @ACCOUNT_CD
              AND a.benchmark_cd = b.benchmark_cd)
BEGIN
  UPDATE #POSITION
     SET bmk_wgt = bw.weight
    FROM account a, equity_common..benchmark_weight bw
   WHERE a.strategy_id = @STRATEGY_ID
     AND a.account_cd = @ACCOUNT_CD
     AND bw.reference_date = @BDATE
     AND bw.reference_date = bw.effective_date
     AND bw.acct_cd = a.benchmark_cd
     AND #POSITION.security_id = bw.security_id
END
ELSE
BEGIN
  UPDATE #POSITION
     SET bmk_wgt = p.weight / 100.0
    FROM account a, universe_def d, universe_makeup p
   WHERE a.strategy_id = @STRATEGY_ID
     AND a.account_cd = @ACCOUNT_CD
     AND a.benchmark_cd = d.universe_cd
     AND d.universe_id = p.universe_id
     AND p.universe_dt = @BDATE
     AND #POSITION.security_id = p.security_id
END

UPDATE #POSITION
   SET bmk_wgt = 0.0
 WHERE bmk_wgt IS NULL

IF @DEBUG = 1
BEGIN
  SELECT '#POSITION'
  SELECT * FROM #POSITION ORDER BY security_id
END

DECLARE @PREV_BDATE datetime

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

SELECT @PREV_BDATE AS [From],
       @BDATE AS [To]

CREATE TABLE #RESULT (
  security_id	int				NULL,
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

  acct_bmk_wgt	float			NULL,

  curr_total_score		float	NULL,
  prev_total_score		float	NULL,
  change_total_score	float	NULL
)

DECLARE @UNIVERSE_ID int

IF @REPORT_VIEW = 'RANKS'
BEGIN
  INSERT #RESULT (security_id)
  SELECT p.security_id
    FROM strategy g, universe_makeup p
   WHERE g.strategy_id = @STRATEGY_ID
     AND p.universe_dt = @BDATE
     AND g.universe_id = p.universe_id

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

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT: RANKS (1)'
    SELECT * FROM #RESULT ORDER BY security_id
  END

  DELETE #RESULT WHERE prev_total_score IS NULL

  UPDATE #RESULT
     SET change_total_score = curr_total_score - prev_total_score

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT: RANKS (2)'
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
    SELECT '#RESULT: RANKS (3)'
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
       AND d2.universe_cd = d1.universe_cd + '_MPF_CAP' --DOES NOT MATTER EQL OR CAP WEIGHTED, JUST NEED CONSTITUENTS
  END
  ELSE IF @REPORT_VIEW = 'UNIVERSE'
  BEGIN
    SELECT @UNIVERSE_ID = universe_id
      FROM strategy
     WHERE strategy_id = @STRATEGY_ID
  END

  IF @DEBUG = 1
    BEGIN SELECT '@UNIVERSE_ID', @UNIVERSE_ID END

  INSERT #RESULT (security_id)
  SELECT security_id
    FROM universe_makeup
   WHERE universe_dt = @BDATE
     AND universe_id = @UNIVERSE_ID
     AND security_id NOT IN (SELECT security_id FROM universe_makeup
                              WHERE universe_dt = @PREV_BDATE
                                AND universe_id = @UNIVERSE_ID)

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT: MODEL OR UNIVERSE (1)'
    SELECT * FROM #RESULT ORDER BY security_id
  END

  INSERT #RESULT (security_id)
  SELECT security_id
    FROM universe_makeup
   WHERE universe_dt = @PREV_BDATE
     AND universe_id = @UNIVERSE_ID
     AND security_id NOT IN (SELECT security_id FROM universe_makeup
                              WHERE universe_dt = @BDATE
                                AND universe_id = @UNIVERSE_ID)

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT: MODEL OR UNIVERSE (2)'
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
    SELECT '#RESULT: MODEL OR UNIVERSE (3)'
    SELECT * FROM #RESULT ORDER BY security_id
  END
END

UPDATE #RESULT
   SET ticker = y.ticker,
       cusip = y.cusip,
       sedol = y.sedol,
       isin = y.isin,
       imnt_nm = y.security_name,
       country_cd = y.issue_country_cd
  FROM equity_common..security y
 WHERE #RESULT.security_id = y.security_id

UPDATE #RESULT
   SET country_nm = UPPER(c.country_name)
  FROM equity_common..country c
 WHERE #RESULT.country_cd = c.country_cd

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

UPDATE #RESULT
   SET acct_bmk_wgt = p.weight - p.bmk_wgt
  FROM #POSITION p
 WHERE #RESULT.security_id = p.security_id

IF EXISTS (SELECT 1 FROM account a, benchmark b
            WHERE a.strategy_id = @STRATEGY_ID
              AND a.account_cd = @ACCOUNT_CD
              AND a.benchmark_cd = b.benchmark_cd)
BEGIN
  UPDATE #RESULT
     SET acct_bmk_wgt = bw.weight * -1.0
    FROM account a, equity_common..benchmark_weight bw
  WHERE a.strategy_id = @STRATEGY_ID
     AND a.account_cd = @ACCOUNT_CD
     AND bw.reference_date = @BDATE
     AND bw.reference_date = bw.effective_date
     AND bw.acct_cd = a.benchmark_cd
     AND #RESULT.security_id = bw.security_id
     AND #RESULT.acct_bmk_wgt IS NULL
END
ELSE
BEGIN
  UPDATE #RESULT
     SET acct_bmk_wgt = (p.weight / 100.0) * -1.0
    FROM account a, universe_def d, universe_makeup p
   WHERE a.strategy_id = @STRATEGY_ID
     AND a.account_cd = @ACCOUNT_CD
     AND a.benchmark_cd = d.universe_cd
     AND d.universe_id = p.universe_id
     AND p.universe_dt = @BDATE
     AND #RESULT.security_id = p.security_id
     AND #RESULT.acct_bmk_wgt IS NULL
END

UPDATE #RESULT
   SET acct_bmk_wgt = 0.0
 WHERE acct_bmk_wgt IS NULL

DROP TABLE #POSITION

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT: FINAL STATE'
  SELECT * FROM #RESULT ORDER BY change_total_score DESC, curr_total_score DESC
END

DECLARE @SQL varchar(1500),
        @SQL2 varchar(1500)

SELECT @SQL = 'SELECT ticker AS [Ticker], '
SELECT @SQL = @SQL + 'cusip AS [CUSIP], '
SELECT @SQL = @SQL + 'sedol AS [SEDOL], '
SELECT @SQL = @SQL + 'isin AS [ISIN], '
SELECT @SQL = @SQL + 'imnt_nm AS [Name], '
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
    SELECT @SQL = @SQL + 'ORDER BY change_total_score DESC, cusip, sedol'

    SELECT @SQL2 = @SQL2 + 'WHERE change_total_score < 0.0 '
    SELECT @SQL2 = @SQL2 + 'ORDER BY change_total_score, cusip, sedol'
  END
  ELSE
  BEGIN
    SELECT @SQL = @SQL + 'WHERE change_total_score < 0.0 '
    SELECT @SQL = @SQL + 'ORDER BY change_total_score, cusip, sedol'

    SELECT @SQL2 = @SQL2 + 'WHERE change_total_score >= 0.0 '
    SELECT @SQL2 = @SQL2 + 'ORDER BY change_total_score DESC, cusip, sedol'
  END
END
ELSE IF @REPORT_VIEW = 'MODEL'
BEGIN
  IF EXISTS (SELECT * FROM strategy WHERE strategy_id = @STRATEGY_ID AND rank_order = 1)
  BEGIN
    SELECT @SQL = @SQL + 'WHERE change_total_score >= 0.0 '
    SELECT @SQL = @SQL + 'OR (change_total_score IS NULL AND curr_total_score IS NOT NULL) '
    SELECT @SQL = @SQL + 'ORDER BY curr_total_score DESC, change_total_score DESC, cusip, sedol'

    SELECT @SQL2 = @SQL2 + 'WHERE change_total_score < 0.0 '
    SELECT @SQL2 = @SQL2 + 'OR (change_total_score IS NULL AND curr_total_score IS NULL) '
    SELECT @SQL2 = @SQL2 + 'ORDER BY prev_total_score DESC, change_total_score, cusip, sedol'
  END
  ELSE
  BEGIN
    SELECT @SQL = @SQL + 'WHERE change_total_score < 0.0 '
    SELECT @SQL = @SQL + 'OR (change_total_score IS NULL AND curr_total_score IS NULL) '
    SELECT @SQL = @SQL + 'ORDER BY prev_total_score DESC, change_total_score, cusip, sedol'

    SELECT @SQL2 = @SQL2 + 'WHERE change_total_score >= 0.0 '
    SELECT @SQL2 = @SQL2 + 'OR (change_total_score IS NULL AND curr_total_score IS NOT NULL) '
    SELECT @SQL2 = @SQL2 + 'ORDER BY curr_total_score DESC, change_total_score DESC, cusip, sedol'
  END
END
ELSE IF @REPORT_VIEW = 'UNIVERSE'
BEGIN
  SELECT @SQL = @SQL + 'WHERE curr_total_score IS NOT NULL '
  SELECT @SQL2 = @SQL2 + 'WHERE curr_total_score IS NULL '

  IF EXISTS (SELECT * FROM strategy WHERE strategy_id = @STRATEGY_ID AND rank_order = 1)
  BEGIN
     SELECT @SQL = @SQL + 'ORDER BY curr_total_score DESC'
     SELECT @SQL2 = @SQL2 + 'ORDER BY prev_total_score DESC'
  END
  ELSE
  BEGIN
     SELECT @SQL = @SQL + 'ORDER BY curr_total_score'
     SELECT @SQL2 = @SQL2 + 'ORDER BY prev_total_score'
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
  CREATE TABLE #RANK_PARAMS (
    factor_id	int			NULL,
    against		varchar(1)	NULL,
    against_id	int			NULL,
    weight		float		NULL
  )

  INSERT #RANK_PARAMS
  SELECT w.factor_id, w.against, NULL, w.weight
    FROM factor_against_weight w, strategy g
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND w.against = 'U'

  INSERT #RANK_PARAMS
  SELECT w.factor_id, w.against, w.against_id, w.weight
    FROM factor_against_weight w, strategy g
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND w.against = 'C'
     AND w.against_id IN (SELECT DISTINCT sector_id FROM #RESULT WHERE sector_id IS NOT NULL)

  INSERT #RANK_PARAMS
  SELECT w.factor_id, w.against, w.against_id, w.weight
    FROM factor_against_weight w, strategy g
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND w.against = 'G'
     AND w.against_id IN (SELECT DISTINCT segment_id FROM #RESULT WHERE segment_id IS NOT NULL)

  IF @DEBUG = 1
  BEGIN
    SELECT '#RANK_PARAMS'
    SELECT * FROM #RANK_PARAMS ORDER BY against, against_id, factor_id
  END

  CREATE TABLE #RESULT2 (
    security_id		int		NULL,

    factor_id		int				NULL,
    factor_cd		varchar(32)		NULL,
    factor_short_nm	varchar(64)		NULL,
    factor_nm		varchar(255)	NULL,

    against			varchar(1)	NULL,
    against_id		int			NULL,
    weight			float		NULL,

    curr_rank		int		NULL,
    prev_rank		int		NULL,
    change_rank		int		NULL
  )

  SELECT @UNIVERSE_ID = universe_id
    FROM strategy
   WHERE strategy_id = @STRATEGY_ID

  INSERT #RESULT2
        (security_id, factor_id, factor_cd, factor_short_nm, factor_nm, against, against_id, weight, curr_rank)
  SELECT r.security_id, f.factor_id, f.factor_cd, f.factor_short_nm, f.factor_nm, p.against, p.against_id, p.weight, o.rank
    FROM #RANK_PARAMS p, #RESULT r, rank_inputs i, rank_output o, factor f
   WHERE i.bdate = @BDATE
     AND i.universe_id = @UNIVERSE_ID
     AND i.factor_id = p.factor_id
     AND p.factor_id = f.factor_id
     AND i.against = p.against
     AND i.against = 'U'
     AND i.rank_event_id = o.rank_event_id
     AND r.security_id = o.security_id

  INSERT #RESULT2
        (security_id, factor_id, factor_cd, factor_short_nm, factor_nm, against, against_id, weight, curr_rank)
  SELECT r.security_id, f.factor_id, f.factor_cd, f.factor_short_nm, f.factor_nm, p.against, p.against_id, p.weight, o.rank
    FROM #RANK_PARAMS p, #RESULT r, rank_inputs i, rank_output o, factor f
   WHERE i.bdate = @BDATE
     AND i.universe_id = @UNIVERSE_ID
     AND i.factor_id = p.factor_id
     AND p.factor_id = f.factor_id
     AND i.against = p.against
     AND i.against = 'C'
     AND i.against_id = p.against_id
     AND i.rank_event_id = o.rank_event_id
     AND r.security_id = o.security_id

  INSERT #RESULT2
        (security_id, factor_id, factor_cd, factor_short_nm, factor_nm, against, against_id, weight, curr_rank)
  SELECT r.security_id, f.factor_id, f.factor_cd, f.factor_short_nm, f.factor_nm, p.against, p.against_id, p.weight, o.rank
    FROM #RANK_PARAMS p, #RESULT r, rank_inputs i, rank_output o, factor f
   WHERE i.bdate = @BDATE
     AND i.universe_id = @UNIVERSE_ID
     AND i.factor_id = p.factor_id
     AND p.factor_id = f.factor_id
     AND i.against = p.against
     AND i.against = 'G'
     AND i.against_id = p.against_id
     AND i.rank_event_id = o.rank_event_id
     AND r.security_id = o.security_id

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT2 (1)'
    SELECT * FROM #RESULT2 ORDER BY security_id, against, factor_id
  END

  UPDATE #RESULT2
     SET prev_rank = o.rank
    FROM rank_inputs i, rank_output o
   WHERE i.bdate = @PREV_BDATE
     AND i.universe_id = @UNIVERSE_ID
     AND i.factor_id = #RESULT2.factor_id
     AND i.against = #RESULT2.against
     AND i.against = 'U'
     AND i.rank_event_id = o.rank_event_id
     AND #RESULT2.security_id = o.security_id

  UPDATE #RESULT2
     SET prev_rank = o.rank
    FROM rank_inputs i, rank_output o
   WHERE i.bdate = @PREV_BDATE
     AND i.universe_id = @UNIVERSE_ID
     AND i.factor_id = #RESULT2.factor_id
     AND i.against = #RESULT2.against
     AND i.against = 'C'
     AND i.against_id = #RESULT2.against_id
     AND i.rank_event_id = o.rank_event_id
     AND #RESULT2.security_id = o.security_id

  UPDATE #RESULT2
     SET prev_rank = o.rank
    FROM rank_inputs i, rank_output o
   WHERE i.bdate = @PREV_BDATE
     AND i.universe_id = @UNIVERSE_ID
     AND i.factor_id = #RESULT2.factor_id
     AND i.against = #RESULT2.against
     AND i.against = 'G'
     AND i.against_id = #RESULT2.against_id
     AND i.rank_event_id = o.rank_event_id
     AND #RESULT2.security_id = o.security_id

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT2 (2)'
    SELECT * FROM #RESULT2 ORDER BY security_id, against, factor_id
  END

  INSERT #RESULT2
        (security_id, factor_id, factor_cd, factor_short_nm, factor_nm, against, against_id, weight, prev_rank)
  SELECT r.security_id, f.factor_id, f.factor_cd, f.factor_short_nm, f.factor_nm, p.against, p.against_id, p.weight, o.rank
    FROM #RANK_PARAMS p, #RESULT r, rank_inputs i, rank_output o, factor f
   WHERE i.bdate = @PREV_BDATE
     AND i.universe_id = @UNIVERSE_ID
     AND i.factor_id = p.factor_id
     AND p.factor_id = f.factor_id
     AND i.against = p.against
     AND i.against = 'U'
     AND i.rank_event_id = o.rank_event_id
     AND r.security_id = o.security_id
     AND r.security_id NOT IN (SELECT security_id FROM #RESULT2 WHERE curr_rank IS NOT NULL)

  INSERT #RESULT2
        (security_id, factor_id, factor_cd, factor_short_nm, factor_nm, against, against_id, weight, prev_rank)
  SELECT r.security_id, f.factor_id, f.factor_cd, f.factor_short_nm, f.factor_nm, p.against, p.against_id, p.weight, o.rank
    FROM #RANK_PARAMS p, #RESULT r, rank_inputs i, rank_output o, factor f
   WHERE i.bdate = @PREV_BDATE
     AND i.universe_id = @UNIVERSE_ID
     AND i.factor_id = p.factor_id
     AND p.factor_id = f.factor_id
     AND i.against = p.against
     AND i.against = 'C'
     AND i.against_id = p.against_id
     AND i.rank_event_id = o.rank_event_id
     AND r.security_id = o.security_id
     AND r.security_id NOT IN (SELECT security_id FROM #RESULT2 WHERE curr_rank IS NOT NULL)

  INSERT #RESULT2
        (security_id, factor_id, factor_cd, factor_short_nm, factor_nm, against, against_id, weight, prev_rank)
  SELECT r.security_id, f.factor_id, f.factor_cd, f.factor_short_nm, f.factor_nm, p.against, p.against_id, p.weight, o.rank
    FROM #RANK_PARAMS p, #RESULT r, rank_inputs i, rank_output o, factor f
   WHERE i.bdate = @PREV_BDATE
     AND i.universe_id = @UNIVERSE_ID
     AND i.factor_id = p.factor_id
     AND p.factor_id = f.factor_id
     AND i.against = p.against
     AND i.against = 'G'
     AND i.against_id = p.against_id
     AND i.rank_event_id = o.rank_event_id
     AND r.security_id = o.security_id
     AND r.security_id NOT IN (SELECT security_id FROM #RESULT2 WHERE curr_rank IS NOT NULL)

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT2 (3)'
    SELECT * FROM #RESULT2 ORDER BY security_id, against, factor_id
  END

  --OVERRIDE WEIGHT LOGIC: BEGIN
  UPDATE #RESULT2
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RESULT2.factor_id = o.factor_id
     AND #RESULT2.against = o.against
     AND #RESULT2.against = 'U'
     AND o.level_type = 'G'
     AND o.level_id = r.segment_id
     and #RESULT2.security_id = r.security_id
  UPDATE #RESULT2
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RESULT2.factor_id = o.factor_id
     AND #RESULT2.against = o.against
     AND #RESULT2.against = 'C'
     AND (r.sector_id = o.against_id OR (r.sector_id IS NULL AND o.against_id IS NULL))
     AND o.level_type = 'G'
     AND o.level_id = r.segment_id
     AND #RESULT2.security_id = r.security_id
  UPDATE #RESULT2
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RESULT2.factor_id = o.factor_id
     AND #RESULT2.against = o.against
     AND #RESULT2.against = 'G'
     AND (r.segment_id = o.against_id OR (r.segment_id IS NULL AND o.against_id IS NULL))
     AND o.level_type = 'G'
     AND o.level_id = r.segment_id
     AND #RESULT2.security_id = r.security_id

  UPDATE #RESULT2
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RESULT2.factor_id = o.factor_id
     AND #RESULT2.against = o.against
     AND #RESULT2.against = 'U'
     AND o.level_type = 'C'
     AND o.level_id = r.sector_id
     and #RESULT2.security_id = r.security_id
  UPDATE #RESULT2
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RESULT2.factor_id = o.factor_id
     AND #RESULT2.against = o.against
     AND #RESULT2.against = 'C'
     AND (r.sector_id = o.against_id OR (r.sector_id IS NULL AND o.against_id IS NULL))
     AND o.level_type = 'C'
     AND o.level_id = r.sector_id
     AND #RESULT2.security_id = r.security_id
  UPDATE #RESULT2
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RESULT2.factor_id = o.factor_id
     AND #RESULT2.against = o.against
     AND #RESULT2.against = 'G'
     AND (r.segment_id = o.against_id OR (r.segment_id IS NULL AND o.against_id IS NULL))
     AND o.level_type = 'C'
     AND o.level_id = r.sector_id
     AND #RESULT2.security_id = r.security_id

  UPDATE #RESULT2
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RESULT2.factor_id = o.factor_id
     AND #RESULT2.against = o.against
     AND #RESULT2.against = 'U'
     AND o.level_type = 'U'
     and #RESULT2.security_id = r.security_id
  UPDATE #RESULT2
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RESULT2.factor_id = o.factor_id
     AND #RESULT2.against = o.against
     AND #RESULT2.against = 'C'
     AND (r.sector_id = o.against_id OR (r.sector_id IS NULL AND o.against_id IS NULL))
     AND o.level_type = 'U'
     AND #RESULT2.security_id = r.security_id
  UPDATE #RESULT2
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RESULT2.factor_id = o.factor_id
     AND #RESULT2.against = o.against
     AND #RESULT2.against = 'G'
     AND (r.segment_id = o.against_id OR (r.segment_id IS NULL AND o.against_id IS NULL))
     AND o.level_type = 'U'
     AND #RESULT2.security_id = r.security_id
  --OVERRIDE WEIGHT LOGIC: END

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT2 (4): AFTER OVERRIDE WEIGHT UPDATE'
    SELECT * FROM #RESULT2 ORDER BY security_id, against, factor_id
  END

  DELETE #RESULT2 WHERE weight = 0.0

  UPDATE #RESULT2
     SET weight = weight * w.segment_ss_wgt * w.ss_total_wgt
    FROM #RESULT r, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND #RESULT2.security_id = r.security_id
     AND (r.sector_id = w.sector_id OR (r.sector_id IS NULL AND w.sector_id IS NULL))
     AND (r.segment_id = w.segment_id OR (r.segment_id IS NULL AND w.segment_id IS NULL))
     AND #RESULT2.against = 'G'
  UPDATE #RESULT2
     SET weight = weight * w.sector_ss_wgt * w.ss_total_wgt
    FROM #RESULT r, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND #RESULT2.security_id = r.security_id
     AND (r.sector_id = w.sector_id OR (r.sector_id IS NULL AND w.sector_id IS NULL))
     AND (r.segment_id = w.segment_id OR (r.segment_id IS NULL AND w.segment_id IS NULL))
     AND #RESULT2.against = 'C'
  UPDATE #RESULT2
     SET weight = weight * w.universe_total_wgt
    FROM #RESULT r, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND #RESULT2.security_id = r.security_id
     AND (r.sector_id = w.sector_id OR (r.sector_id IS NULL AND w.sector_id IS NULL))
     AND (r.segment_id = w.segment_id OR (r.segment_id IS NULL AND w.segment_id IS NULL))
     AND #RESULT2.against = 'U'

  UPDATE #RESULT2
     SET change_rank = curr_rank - prev_rank

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT2 (5)'
    SELECT * FROM #RESULT2 ORDER BY security_id, against, factor_id
  END

  SELECT @SQL = 'SELECT r.ticker AS [Ticker], '
  SELECT @SQL = @SQL + 'r.cusip AS [CUSIP], '
  SELECT @SQL = @SQL + 'r.sedol AS [SEDOL], '
  SELECT @SQL = @SQL + 'r.isin AS [ISIN], '
  SELECT @SQL = @SQL + 'r2.factor_short_nm AS [Factor], '
  SELECT @SQL = @SQL + 'r2.factor_nm AS [Factor Name], '
  SELECT @SQL = @SQL + 'CASE r2.against WHEN ''U'' THEN ''UNIVERSE'' '
  SELECT @SQL = @SQL + 'WHEN ''C'' THEN ''SECTOR'' '
  SELECT @SQL = @SQL + 'WHEN ''G'' THEN ''SEGMENT'' END AS [Relative To], '
  SELECT @SQL = @SQL + 'r2.weight AS [Weight], '
  SELECT @SQL = @SQL + 'r2.curr_rank AS [Current Rank], '
  SELECT @SQL = @SQL + 'r2.prev_rank AS [Previous Rank], '
  SELECT @SQL = @SQL + 'r2.change_rank AS [Rank Change] '
  SELECT @SQL = @SQL + 'FROM #RESULT r, #RESULT2 r2 '
  SELECT @SQL = @SQL + 'WHERE r.security_id = r2.security_id '

  IF EXISTS (SELECT * FROM strategy WHERE strategy_id = @STRATEGY_ID AND rank_order = 1)
    BEGIN SELECT @SQL = @SQL + 'ORDER BY r.cusip, r.sedol, r2.change_rank DESC, r2.curr_rank DESC' END
  ELSE
    BEGIN SELECT @SQL = @SQL + 'ORDER BY r.cusip, r.sedol, r2.change_rank, r2.curr_rank' END

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
