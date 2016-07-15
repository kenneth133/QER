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
*   1. THIS PROCEDURE DOES NOT HANDLE INTERNATIONAL SECURITIES - IT JOINS ON CUSIP ONLY
*   2. THIS PROCEDURE WILL NOT HANDLE SITUATION WHERE
*      - THE FACTOR MODEL OR SECTOR MODEL HAS CHANGED BETWEEN THE TWO DATES;
*          DATABASE CONTAINS LATEST MODELS ONLY (I.E. LOOKS AT PREV_BDATE WITH CURRENT MODELS)
*      - SECURITY HAS CHANGED CLASSIFICATION IN RUSSELL AND/OR GICS SECTOR MODEL(S)
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
  cusip		varchar(32)	NULL,
  sedol		varchar(32)	NULL,
  units		float		NULL,
  price		float		NULL,
  mval		float		NULL,
  weight	float		NULL,
  bmk_wgt	float		NULL
)

INSERT #POSITION (cusip, sedol, units)
SELECT cusip, sedol, units
  FROM position
 WHERE bdate = @BDATE
   AND account_cd = @ACCOUNT_CD

UPDATE #POSITION
   SET price = i.price_close
  FROM instrument_characteristics i
 WHERE i.bdate = @BDATE
   AND #POSITION.cusip = i.cusip

UPDATE #POSITION
   SET price = 1.0
 WHERE cusip = '_USD'

UPDATE #POSITION
   SET price = 0.0
 WHERE price IS NULL

UPDATE #POSITION
   SET mval = units * price

UPDATE #POSITION
   SET weight = mval / x.tot_mval
  FROM (SELECT SUM(mval) AS tot_mval FROM #POSITION) x

UPDATE #POSITION
   SET bmk_wgt = p.weight / 100.0
  FROM account a, universe_makeup p
 WHERE a.strategy_id = @STRATEGY_ID
   AND a.account_cd = @ACCOUNT_CD
   AND p.universe_dt = @BDATE
   AND p.universe_id = a.bm_universe_id
   AND #POSITION.cusip = p.cusip

UPDATE #POSITION
   SET bmk_wgt = 0.0
 WHERE bmk_wgt IS NULL

IF @DEBUG = 1
BEGIN
  SELECT '#POSITION'
  SELECT * FROM #POSITION ORDER BY cusip, sedol
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
  mqa_id		varchar(32)	NULL,
  ticker		varchar(16)	NULL,
  cusip			varchar(32)	NULL,
  sedol			varchar(32)	NULL,
  isin			varchar(64)	NULL,
  imnt_nm		varchar(255)	NULL,

  country_cd		varchar(4)	NULL,
  country_nm		varchar(128)	NULL,
  sector_id		int		NULL,
  sector_nm		varchar(64)	NULL,
  segment_id		int		NULL,
  segment_nm		varchar(128)	NULL,

  acct_bmk_wgt		float		NULL,

  curr_total_score	float		NULL,
  prev_total_score	float		NULL,
  change_total_score	float		NULL
)

DECLARE @UNIVERSE_ID int

IF @REPORT_VIEW = 'RANKS'
BEGIN
  INSERT #RESULT (mqa_id, ticker, cusip, sedol, isin)
  SELECT p.mqa_id, p.ticker, p.cusip, p.sedol, p.isin
    FROM strategy g, universe_makeup p
   WHERE g.strategy_id = @STRATEGY_ID
     AND p.universe_dt = @BDATE
     AND g.universe_id = p.universe_id

  UPDATE #RESULT
     SET curr_total_score = s.total_score
    FROM scores s
   WHERE s.bdate = @BDATE
     AND s.strategy_id = @STRATEGY_ID
     AND #RESULT.cusip = s.cusip

  UPDATE #RESULT
     SET prev_total_score = s.total_score
    FROM scores s
   WHERE s.bdate = @PREV_BDATE
     AND s.strategy_id = @STRATEGY_ID
     AND #RESULT.cusip = s.cusip

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT: RANKS (1)'
    SELECT * FROM #RESULT ORDER BY cusip, sedol
  END

  DELETE #RESULT WHERE prev_total_score IS NULL

  UPDATE #RESULT
     SET change_total_score = curr_total_score - prev_total_score

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT: RANKS (2)'
    SELECT * FROM #RESULT ORDER BY cusip, sedol
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
    SELECT * FROM #RESULT ORDER BY cusip, sedol
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

  INSERT #RESULT (mqa_id, ticker, cusip, sedol, isin)
  SELECT mqa_id, ticker, cusip, sedol, isin
    FROM universe_makeup
   WHERE universe_dt = @BDATE
     AND universe_id = @UNIVERSE_ID
     AND cusip NOT IN (SELECT cusip FROM universe_makeup
                        WHERE universe_dt = @PREV_BDATE
                          AND universe_id = @UNIVERSE_ID)

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT: MODEL OR UNIVERSE (1)'
    SELECT * FROM #RESULT ORDER BY cusip, sedol
  END

  INSERT #RESULT (mqa_id, ticker, cusip, sedol, isin)
  SELECT mqa_id, ticker, cusip, sedol, isin
    FROM universe_makeup
   WHERE universe_dt = @PREV_BDATE
     AND universe_id = @UNIVERSE_ID
     AND cusip NOT IN (SELECT cusip FROM universe_makeup
                        WHERE universe_dt = @BDATE
                          AND universe_id = @UNIVERSE_ID)

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT: MODEL OR UNIVERSE (2)'
    SELECT * FROM #RESULT ORDER BY cusip, sedol
  END

  UPDATE #RESULT
     SET curr_total_score = s.total_score
    FROM scores s
   WHERE s.bdate = @BDATE
     AND s.strategy_id = @STRATEGY_ID
     AND #RESULT.cusip = s.cusip

  UPDATE #RESULT
     SET prev_total_score = s.total_score
    FROM scores s
   WHERE s.bdate = @PREV_BDATE
     AND s.strategy_id = @STRATEGY_ID
     AND #RESULT.cusip = s.cusip

  UPDATE #RESULT
     SET change_total_score = curr_total_score - prev_total_score

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT: MODEL OR UNIVERSE (3)'
    SELECT * FROM #RESULT ORDER BY cusip, sedol
  END
END

UPDATE #RESULT
   SET imnt_nm = i.imnt_nm,
       country_cd = i.country
  FROM instrument_characteristics i
 WHERE i.bdate = @BDATE
   AND #RESULT.cusip = i.cusip

UPDATE #RESULT
   SET country_nm = d.decode
  FROM decode d
 WHERE d.item = 'COUNTRY'
   AND #RESULT.country_cd = d.code

UPDATE #RESULT
   SET sector_id = ss.sector_id,
       segment_id = ss.segment_id
  FROM sector_model_security ss, strategy g, factor_model f
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = f.factor_model_id
   AND ss.bdate = @BDATE
   AND ss.sector_model_id = f.sector_model_id
   AND #RESULT.cusip = ss.cusip

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
 WHERE #RESULT.cusip = p.cusip

UPDATE #RESULT
   SET acct_bmk_wgt = (p.weight / 100.0) * -1.0
  FROM account a, universe_makeup p
 WHERE a.strategy_id = @STRATEGY_ID
   AND a.account_cd = @ACCOUNT_CD
   AND p.universe_dt = @BDATE
   AND p.universe_id = a.bm_universe_id
   AND #RESULT.cusip = p.cusip
   AND #RESULT.acct_bmk_wgt IS NULL

UPDATE #RESULT
   SET acct_bmk_wgt = 0.0
 WHERE acct_bmk_wgt IS NULL

DROP TABLE #POSITION

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT: FINAL STATE'
  SELECT * FROM #RESULT ORDER BY change_total_score DESC, curr_total_score DESC
END

IF @REPORT_VIEW IN ('RANKS', 'MODEL')
BEGIN
  IF @REPORT_VIEW = 'RANKS'
  BEGIN
    IF EXISTS (SELECT * FROM strategy WHERE strategy_id = @STRATEGY_ID AND rank_order = 1)
    BEGIN
      SELECT ticker		AS [Ticker],
             cusip		AS [CUSIP],
             sedol		AS [SEDOL],
             isin			AS [ISIN],
             imnt_nm		AS [Name],
             country_nm		AS [Country Name],
             ISNULL(sector_nm, 'UNKNOWN') AS [Sector Name],
             ISNULL(segment_nm, 'UNKNOWN') AS [Segment Name],
             acct_bmk_wgt		AS [Acct-Bmk Wgt],
             curr_total_score	AS [Current],
             prev_total_score	AS [Previous],
             change_total_score	AS [Change]
        FROM #RESULT
       WHERE change_total_score >= 0.0
       ORDER BY change_total_score DESC, cusip, sedol

      SELECT ticker		AS [Ticker],
             cusip		AS [CUSIP],
             sedol		AS [SEDOL],
             isin			AS [ISIN],
             imnt_nm		AS [Name],
             country_nm		AS [Country Name],
             ISNULL(sector_nm, 'UNKNOWN') AS [Sector Name],
             ISNULL(segment_nm, 'UNKNOWN') AS [Segment Name],
             acct_bmk_wgt		AS [Acct-Bmk Wgt],
             curr_total_score	AS [Current],
             prev_total_score	AS [Previous],
             change_total_score	AS [Change]
        FROM #RESULT
       WHERE change_total_score < 0.0
       ORDER BY change_total_score, cusip, sedol
    END
    ELSE
    BEGIN
      SELECT ticker		AS [Ticker],
             cusip		AS [CUSIP],
             sedol		AS [SEDOL],
             isin			AS [ISIN],
             imnt_nm		AS [Name],
             country_nm		AS [Country Name],
             ISNULL(sector_nm, 'UNKNOWN') AS [Sector Name],
             ISNULL(segment_nm, 'UNKNOWN') AS [Segment Name],
             acct_bmk_wgt		AS [Acct-Bmk Wgt],
             curr_total_score	AS [Current],
             prev_total_score	AS [Previous],
             change_total_score	AS [Change]
        FROM #RESULT
       WHERE change_total_score < 0.0
       ORDER BY change_total_score, cusip, sedol

      SELECT ticker		AS [Ticker],
             cusip		AS [CUSIP],
             sedol		AS [SEDOL],
             isin			AS [ISIN],
             imnt_nm		AS [Name],
             country_nm		AS [Country Name],
             ISNULL(sector_nm, 'UNKNOWN') AS [Sector Name],
             ISNULL(segment_nm, 'UNKNOWN') AS [Segment Name],
             acct_bmk_wgt		AS [Acct-Bmk Wgt],
             curr_total_score	AS [Current],
             prev_total_score	AS [Previous],
             change_total_score	AS [Change]
        FROM #RESULT
       WHERE change_total_score >= 0.0
       ORDER BY change_total_score DESC, cusip, sedol
    END
  END
  ELSE IF @REPORT_VIEW = 'MODEL'
  BEGIN
    IF EXISTS (SELECT * FROM strategy WHERE strategy_id = @STRATEGY_ID AND rank_order = 1)
    BEGIN
      SELECT ticker		AS [Ticker],
             cusip		AS [CUSIP],
             sedol		AS [SEDOL],
             isin			AS [ISIN],
             imnt_nm		AS [Name],
             country_nm		AS [Country Name],
             ISNULL(sector_nm, 'UNKNOWN') AS [Sector Name],
             ISNULL(segment_nm, 'UNKNOWN') AS [Segment Name],
             acct_bmk_wgt		AS [Acct-Bmk Wgt],
             curr_total_score	AS [Current],
             prev_total_score	AS [Previous],
             change_total_score	AS [Change]
        FROM #RESULT
       WHERE change_total_score >= 0.0
          OR (change_total_score IS NULL AND curr_total_score IS NOT NULL)
       ORDER BY curr_total_score DESC, change_total_score DESC, cusip, sedol

      SELECT ticker		AS [Ticker],
             cusip		AS [CUSIP],
             sedol		AS [SEDOL],
             isin			AS [ISIN],
             imnt_nm		AS [Name],
             country_nm		AS [Country Name],
             ISNULL(sector_nm, 'UNKNOWN') AS [Sector Name],
             ISNULL(segment_nm, 'UNKNOWN') AS [Segment Name],
             acct_bmk_wgt		AS [Acct-Bmk Wgt],
             curr_total_score	AS [Current],
             prev_total_score	AS [Previous],
             change_total_score	AS [Change]
        FROM #RESULT
       WHERE change_total_score < 0.0
          OR (change_total_score IS NULL AND curr_total_score IS NULL)
       ORDER BY prev_total_score DESC, change_total_score, cusip, sedol
    END
    ELSE
    BEGIN
      SELECT ticker		AS [Ticker],
             cusip		AS [CUSIP],
             sedol		AS [SEDOL],
             isin			AS [ISIN],
             imnt_nm		AS [Name],
             country_nm		AS [Country Name],
             ISNULL(sector_nm, 'UNKNOWN') AS [Sector Name],
             ISNULL(segment_nm, 'UNKNOWN') AS [Segment Name],
             acct_bmk_wgt		AS [Acct-Bmk Wgt],
             curr_total_score	AS [Current],
             prev_total_score	AS [Previous],
             change_total_score	AS [Change]
        FROM #RESULT
       WHERE change_total_score < 0.0
          OR (change_total_score IS NULL AND curr_total_score IS NOT NULL)
       ORDER BY prev_total_score DESC, change_total_score, cusip, sedol

      SELECT ticker		AS [Ticker],
             cusip		AS [CUSIP],
             sedol		AS [SEDOL],
             isin			AS [ISIN],
             imnt_nm		AS [Name],
             country_nm		AS [Country Name],
             ISNULL(sector_nm, 'UNKNOWN') AS [Sector Name],
             ISNULL(segment_nm, 'UNKNOWN') AS [Segment Name],
             acct_bmk_wgt		AS [Acct-Bmk Wgt],
             curr_total_score	AS [Current],
             prev_total_score	AS [Previous],
             change_total_score	AS [Change]
        FROM #RESULT
       WHERE change_total_score >= 0.0
          OR (change_total_score IS NULL AND curr_total_score IS NULL)
       ORDER BY curr_total_score DESC, change_total_score DESC, cusip, sedol
    END
  END

  CREATE TABLE #RANK_PARAMS (
    factor_id	int		NULL,
    against	varchar(1)	NULL,
    against_id	int		NULL,
    weight	float		NULL
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
    mqa_id		varchar(32)	NULL,
    ticker		varchar(16)	NULL,
    cusip		varchar(32)	NULL,
    sedol		varchar(32)	NULL,
    isin		varchar(64)	NULL,

    factor_id		int		NULL,
    factor_cd		varchar(32)	NULL,
    factor_short_nm	varchar(64)	NULL,
    factor_nm		varchar(255)	NULL,

    against		varchar(1)	NULL,
    against_id		int		NULL,
    weight		float		NULL,

    curr_rank		int		NULL,
    prev_rank		int		NULL,
    change_rank		int		NULL
  )

  SELECT @UNIVERSE_ID = universe_id
    FROM strategy
   WHERE strategy_id = @STRATEGY_ID

  INSERT #RESULT2
        (mqa_id, ticker, cusip, sedol, isin, factor_id, factor_cd, factor_short_nm, factor_nm, against, against_id, weight, curr_rank)
  SELECT r.mqa_id, r.ticker, r.cusip, r.sedol, r.isin, f.factor_id, f.factor_cd, f.factor_short_nm, f.factor_nm, p.against, p.against_id, p.weight, o.rank
    FROM #RANK_PARAMS p, #RESULT r, rank_inputs i, rank_output o, factor f
   WHERE i.bdate = @BDATE
     AND i.universe_id = @UNIVERSE_ID
     AND i.factor_id = p.factor_id
     AND p.factor_id = f.factor_id
     AND i.against = p.against
     AND i.against = 'U'
     AND i.rank_event_id = o.rank_event_id
     AND r.cusip = o.cusip

  INSERT #RESULT2
        (mqa_id, ticker, cusip, sedol, isin, factor_id, factor_cd, factor_short_nm, factor_nm, against, against_id, weight, curr_rank)
  SELECT r.mqa_id, r.ticker, r.cusip, r.sedol, r.isin, f.factor_id, f.factor_cd, f.factor_short_nm, f.factor_nm, p.against, p.against_id, p.weight, o.rank
    FROM #RANK_PARAMS p, #RESULT r, rank_inputs i, rank_output o, factor f
   WHERE i.bdate = @BDATE
     AND i.universe_id = @UNIVERSE_ID
     AND i.factor_id = p.factor_id
     AND p.factor_id = f.factor_id
     AND i.against = p.against
     AND i.against = 'C'
     AND i.against_id = p.against_id
     AND i.rank_event_id = o.rank_event_id
     AND r.cusip = o.cusip

  INSERT #RESULT2
        (mqa_id, ticker, cusip, sedol, isin, factor_id, factor_cd, factor_short_nm, factor_nm, against, against_id, weight, curr_rank)
  SELECT r.mqa_id, r.ticker, r.cusip, r.sedol, r.isin, f.factor_id, f.factor_cd, f.factor_short_nm, f.factor_nm, p.against, p.against_id, p.weight, o.rank
    FROM #RANK_PARAMS p, #RESULT r, rank_inputs i, rank_output o, factor f
   WHERE i.bdate = @BDATE
     AND i.universe_id = @UNIVERSE_ID
     AND i.factor_id = p.factor_id
     AND p.factor_id = f.factor_id
     AND i.against = p.against
     AND i.against = 'G'
     AND i.against_id = p.against_id
     AND i.rank_event_id = o.rank_event_id
     AND r.cusip = o.cusip

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT2 (1)'
    SELECT * FROM #RESULT2 ORDER BY cusip, sedol, against, factor_id
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
     AND #RESULT2.cusip = o.cusip

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
     AND #RESULT2.cusip = o.cusip

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
     AND #RESULT2.cusip = o.cusip

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT2 (2)'
    SELECT * FROM #RESULT2 ORDER BY cusip, sedol, against, factor_id
  END

  INSERT #RESULT2
        (mqa_id, ticker, cusip, sedol, isin, factor_id, factor_cd, factor_short_nm, factor_nm, against, against_id, weight, prev_rank)
  SELECT r.mqa_id, r.ticker, r.cusip, r.sedol, r.isin, f.factor_id, f.factor_cd, f.factor_short_nm, f.factor_nm, p.against, p.against_id, p.weight, o.rank
    FROM #RANK_PARAMS p, #RESULT r, rank_inputs i, rank_output o, factor f
   WHERE i.bdate = @PREV_BDATE
     AND i.universe_id = @UNIVERSE_ID
     AND i.factor_id = p.factor_id
     AND p.factor_id = f.factor_id
     AND i.against = p.against
     AND i.against = 'U'
     AND i.rank_event_id = o.rank_event_id
     AND r.cusip = o.cusip
     AND r.cusip NOT IN (SELECT DISTINCT cusip FROM #RESULT2 WHERE curr_rank IS NOT NULL)

  INSERT #RESULT2
        (mqa_id, ticker, cusip, sedol, isin, factor_id, factor_cd, factor_short_nm, factor_nm, against, against_id, weight, prev_rank)
  SELECT r.mqa_id, r.ticker, r.cusip, r.sedol, r.isin, f.factor_id, f.factor_cd, f.factor_short_nm, f.factor_nm, p.against, p.against_id, p.weight, o.rank
    FROM #RANK_PARAMS p, #RESULT r, rank_inputs i, rank_output o, factor f
   WHERE i.bdate = @PREV_BDATE
     AND i.universe_id = @UNIVERSE_ID
     AND i.factor_id = p.factor_id
     AND p.factor_id = f.factor_id
     AND i.against = p.against
     AND i.against = 'C'
     AND i.against_id = p.against_id
     AND i.rank_event_id = o.rank_event_id
     AND r.cusip = o.cusip
     AND r.cusip NOT IN (SELECT DISTINCT cusip FROM #RESULT2 WHERE curr_rank IS NOT NULL)

  INSERT #RESULT2
        (mqa_id, ticker, cusip, sedol, isin, factor_id, factor_cd, factor_short_nm, factor_nm, against, against_id, weight, prev_rank)
  SELECT r.mqa_id, r.ticker, r.cusip, r.sedol, r.isin, f.factor_id, f.factor_cd, f.factor_short_nm, f.factor_nm, p.against, p.against_id, p.weight, o.rank
    FROM #RANK_PARAMS p, #RESULT r, rank_inputs i, rank_output o, factor f
   WHERE i.bdate = @PREV_BDATE
     AND i.universe_id = @UNIVERSE_ID
     AND i.factor_id = p.factor_id
     AND p.factor_id = f.factor_id
     AND i.against = p.against
     AND i.against = 'G'
     AND i.against_id = p.against_id
     AND i.rank_event_id = o.rank_event_id
     AND r.cusip = o.cusip
     AND r.cusip NOT IN (SELECT DISTINCT cusip FROM #RESULT2 WHERE curr_rank IS NOT NULL)

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT2 (3)'
    SELECT * FROM #RESULT2 ORDER BY cusip, sedol, against, factor_id
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
     and #RESULT2.cusip = r.cusip
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
     AND #RESULT2.cusip = r.cusip
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
     AND #RESULT2.cusip = r.cusip

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
     and #RESULT2.cusip = r.cusip
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
     AND #RESULT2.cusip = r.cusip
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
     AND #RESULT2.cusip = r.cusip

  UPDATE #RESULT2
     SET weight = o.override_wgt
    FROM #RESULT r, strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RESULT2.factor_id = o.factor_id
     AND #RESULT2.against = o.against
     AND #RESULT2.against = 'U'
     AND o.level_type = 'U'
     and #RESULT2.cusip = r.cusip
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
     AND #RESULT2.cusip = r.cusip
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
     AND #RESULT2.cusip = r.cusip
  --OVERRIDE WEIGHT LOGIC: END

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT2 (4): AFTER OVERRIDE WEIGHT UPDATE'
    SELECT * FROM #RESULT2 ORDER BY cusip, sedol, against, factor_id
  END

  DELETE #RESULT2 WHERE weight = 0.0

  UPDATE #RESULT2
     SET weight = weight * w.segment_ss_wgt * w.ss_total_wgt
    FROM #RESULT r, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND #RESULT2.cusip = r.cusip
     AND (r.sector_id = w.sector_id OR (r.sector_id IS NULL AND w.sector_id IS NULL))
     AND (r.segment_id = w.segment_id OR (r.segment_id IS NULL AND w.segment_id IS NULL))
     AND #RESULT2.against = 'G'
  UPDATE #RESULT2
     SET weight = weight * w.sector_ss_wgt * w.ss_total_wgt
    FROM #RESULT r, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND #RESULT2.cusip = r.cusip
     AND (r.sector_id = w.sector_id OR (r.sector_id IS NULL AND w.sector_id IS NULL))
     AND (r.segment_id = w.segment_id OR (r.segment_id IS NULL AND w.segment_id IS NULL))
     AND #RESULT2.against = 'C'
  UPDATE #RESULT2
     SET weight = weight * w.universe_total_wgt
    FROM #RESULT r, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND #RESULT2.cusip = r.cusip
     AND (r.sector_id = w.sector_id OR (r.sector_id IS NULL AND w.sector_id IS NULL))
     AND (r.segment_id = w.segment_id OR (r.segment_id IS NULL AND w.segment_id IS NULL))
     AND #RESULT2.against = 'U'

  UPDATE #RESULT2
     SET change_rank = curr_rank - prev_rank

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT2 (5)'
    SELECT * FROM #RESULT2 ORDER BY cusip, sedol, against, factor_id
  END

  IF EXISTS (SELECT * FROM strategy WHERE strategy_id = @STRATEGY_ID AND rank_order = 1)
  BEGIN
    SELECT r2.ticker	AS [Ticker],
           r2.cusip	AS [CUSIP],
           r2.sedol	AS [SEDOL],
           r2.isin	AS [ISIN],
           r2.factor_short_nm AS [Factor],
           CASE r2.against WHEN 'U' THEN 'UNIVERSE'
                           WHEN 'C' THEN 'SECTOR'
                           WHEN 'G' THEN 'SEGMENT' END AS [Relative To],
           r2.weight	AS [Weight],
           r2.curr_rank	AS [Current Rank],
           r2.prev_rank	AS [Previous Rank],
           r2.change_rank	AS [Rank Change]
      FROM #RESULT r, #RESULT2 r2
     WHERE r.cusip = r2.cusip
     ORDER BY r.cusip, r.sedol, r2.change_rank DESC, r2.curr_rank DESC
  END
  ELSE
  BEGIN
    SELECT r2.ticker	AS [Ticker],
           r2.cusip	AS [CUSIP],
           r2.sedol	AS [SEDOL],
           r2.isin	AS [ISIN],
           r2.factor_short_nm AS [Factor],
           CASE r2.against WHEN 'U' THEN 'UNIVERSE'
                           WHEN 'C' THEN 'SECTOR'
                           WHEN 'G' THEN 'SEGMENT' END AS [Relative To],
           r2.weight	AS [Weight],
           r2.curr_rank	AS [Current Rank],
           r2.prev_rank	AS [Previous Rank],
           r2.change_rank	AS [Rank Change]
      FROM #RESULT r, #RESULT2 r2
     WHERE r.cusip = r2.cusip
     ORDER BY r.cusip, r.sedol, r2.change_rank, r2.curr_rank
  END

  DROP TABLE #RESULT2
END
ELSE IF @REPORT_VIEW = 'UNIVERSE'
BEGIN
  IF EXISTS (SELECT * FROM strategy WHERE strategy_id = @STRATEGY_ID AND rank_order = 1)
  BEGIN
    SELECT ticker			AS [Ticker],
           cusip			AS [CUSIP],
           sedol			AS [SEDOL],
           isin			AS [ISIN],
           imnt_nm		AS [Name],
           country_nm		AS [Country Name],
           ISNULL(sector_nm, 'UNKNOWN') AS [Sector Name],
           ISNULL(segment_nm, 'UNKNOWN') AS [Segment Name],
           acct_bmk_wgt		AS [Acct-Bmk Wgt],
           curr_total_score	AS [Current],
           prev_total_score	AS [Previous],
           change_total_score	AS [Change]
      FROM #RESULT
     WHERE curr_total_score IS NOT NULL
     ORDER BY curr_total_score DESC

    SELECT ticker			AS [Ticker],
           cusip			AS [CUSIP],
           sedol			AS [SEDOL],
           isin			AS [ISIN],
           imnt_nm		AS [Name],
           country_nm		AS [Country Name],
           ISNULL(sector_nm, 'UNKNOWN') AS [Sector Name],
           ISNULL(segment_nm, 'UNKNOWN') AS [Segment Name],
           acct_bmk_wgt		AS [Acct-Bmk Wgt],
           curr_total_score	AS [Current],
           prev_total_score	AS [Previous],
           change_total_score	AS [Change]
      FROM #RESULT
     WHERE curr_total_score IS NULL
     ORDER BY prev_total_score DESC
  END
  ELSE
  BEGIN
    SELECT ticker			AS [Ticker],
           cusip			AS [CUSIP],
           sedol			AS [SEDOL],
           isin			AS [ISIN],
           imnt_nm		AS [Name],
           country_nm		AS [Country Name],
           ISNULL(sector_nm, 'UNKNOWN') AS [Sector Name],
           ISNULL(segment_nm, 'UNKNOWN') AS [Segment Name],
           acct_bmk_wgt		AS [Acct-Bmk Wgt],
           curr_total_score	AS [Current],
           prev_total_score	AS [Previous],
           change_total_score	AS [Change]
      FROM #RESULT
     WHERE curr_total_score IS NOT NULL
     ORDER BY curr_total_score

    SELECT ticker			AS [Ticker],
           cusip			AS [CUSIP],
           sedol			AS [SEDOL],
           isin			AS [ISIN],
           imnt_nm		AS [Name],
           country_nm		AS [Country Name],
           ISNULL(sector_nm, 'UNKNOWN') AS [Sector Name],
           ISNULL(segment_nm, 'UNKNOWN') AS [Segment Name],
           acct_bmk_wgt		AS [Acct-Bmk Wgt],
           curr_total_score	AS [Current],
           prev_total_score	AS [Previous],
           change_total_score	AS [Change]
      FROM #RESULT
     WHERE curr_total_score IS NULL
     ORDER BY prev_total_score
  END
END

DROP TABLE #RESULT

RETURN 0
go
IF OBJECT_ID('dbo.rpt_changes_to_model') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_changes_to_model >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_changes_to_model >>>'
go
