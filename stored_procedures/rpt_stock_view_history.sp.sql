use QER
go
IF OBJECT_ID('dbo.rpt_stock_view_history') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rpt_stock_view_history
    IF OBJECT_ID('dbo.rpt_stock_view_history') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rpt_stock_view_history >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rpt_stock_view_history >>>'
END
go
CREATE PROCEDURE dbo.rpt_stock_view_history
@BDATE datetime,
@STRATEGY_ID int,
@ACCOUNT_CD varchar(32),
@PERIODS int,
@PERIOD_TYPE varchar(2),
@IDENTIFIER_TYPE varchar(32),
@IDENTIFIER_VALUE varchar(64),
@DEBUG bit = NULL
AS
/* STOCK - HISTORICAL RANKS */

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
  imnt_nm		varchar(100)	NULL,
  currency_cd	varchar(3)		NULL,
  exchange_nm	varchar(60)		NULL,

  region_id		int				NULL,
  region_nm		varchar(128)	NULL,
  country_cd	varchar(50)		NULL,
  country_nm	varchar(128)	NULL,
  sector_id		int				NULL,
  sector_nm		varchar(64)		NULL,
  segment_id	int				NULL,
  segment_nm	varchar(128)	NULL
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

UPDATE #SECURITY
   SET ticker = y.ticker,
       cusip = y.cusip,
       sedol = y.sedol,
       isin = y.isin,
       imnt_nm = y.security_name,
       country_cd = ISNULL(y.domicile_iso_cd, y.issue_country_cd)
  FROM equity_common..security y
 WHERE y.security_id = @SECURITY_ID

UPDATE #SECURITY
   SET country_nm = UPPER(c.country_name)
  FROM equity_common..country c
 WHERE #SECURITY.country_cd = c.country_cd

UPDATE #SECURITY
   SET region_id = d.region_id,
       region_nm = d.region_nm
  FROM strategy g, region_def d, region_makeup p
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.region_model_id = d.region_model_id
   AND d.region_id = p.region_id
   AND #SECURITY.country_cd = p.country_cd

UPDATE #SECURITY
   SET sector_id = ss.sector_id,
       segment_id = ss.segment_id
  FROM sector_model_security ss, strategy g, factor_model f
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = f.factor_model_id
   AND #SECURITY.bdate = ss.bdate
   AND ss.sector_model_id = f.sector_model_id
   AND #SECURITY.security_id = ss.security_id

UPDATE #SECURITY
   SET sector_nm = d.sector_nm
  FROM sector_def d
 WHERE #SECURITY.sector_id = d.sector_id

UPDATE #SECURITY
   SET segment_nm = d.segment_nm
  FROM segment_def d
 WHERE #SECURITY.segment_id = d.segment_id

IF @DEBUG = 1
BEGIN
  SELECT '#SECURITY (3)'
  SELECT * FROM #SECURITY
END

SELECT ticker		AS [Ticker],
       cusip		AS [CUSIP],
       sedol		AS [SEDOL],
       isin			AS [ISIN],
       imnt_nm		AS [Name],
       region_nm	AS [Region Name],
       country_nm	AS [Country Name],
       ISNULL(sector_nm, 'UNKNOWN') AS [Sector Name],
       ISNULL(segment_nm, 'UNKNOWN') AS [Segment Name]
  FROM #SECURITY

CREATE TABLE #RESULT (
  adate				datetime	NULL,
  bdate				datetime	NULL,
  held				bit			NULL,
  acct_bmk_wgt		float		NULL,
  total_score		float		NULL,
  universe_score	float		NULL,
  region_score		float		NULL,
  country_score		float		NULL,
  ss_score			float		NULL,
  sector_score		float		NULL,
  segment_score		float		NULL
)

INSERT #RESULT (adate) VALUES (@BDATE)

WHILE (SELECT COUNT(*) FROM #RESULT) < (ABS(@PERIODS)+1)
BEGIN
  IF @PERIOD_TYPE IN ('YY','YYYY')
    BEGIN INSERT #RESULT (adate) SELECT DATEADD(YY, -1, MIN(adate)) FROM #RESULT END
  ELSE IF @PERIOD_TYPE IN ('QQ','Q')
    BEGIN INSERT #RESULT (adate) SELECT DATEADD(QQ, -1, MIN(adate)) FROM #RESULT END
  ELSE IF @PERIOD_TYPE IN ('MM','M')
    BEGIN INSERT #RESULT (adate) SELECT DATEADD(MM, -1, MIN(adate)) FROM #RESULT END
  ELSE IF @PERIOD_TYPE IN ('WK','WW')
    BEGIN INSERT #RESULT (adate) SELECT DATEADD(WK, -1, MIN(adate)) FROM #RESULT END
  ELSE IF @PERIOD_TYPE IN ('DD','D')
    BEGIN INSERT #RESULT (adate) SELECT DATEADD(DD, -1, MIN(adate)) FROM #RESULT END
END

WHILE EXISTS (SELECT 1 FROM #RESULT WHERE bdate IS NULL)
BEGIN
  SELECT @BDATE = MIN(adate) FROM #RESULT WHERE bdate IS NULL

  EXEC business_date_get @DIFF=0, @REF_DATE=@BDATE, @RET_DATE=@BDATE OUTPUT

  IF @BDATE = (SELECT MIN(adate) FROM #RESULT WHERE bdate IS NULL)
  BEGIN
    UPDATE #RESULT
       SET bdate = adate
     WHERE adate = @BDATE
  END
  ELSE
  BEGIN
    SELECT @BDATE = MIN(adate) FROM #RESULT WHERE bdate IS NULL

    EXEC business_date_get @DIFF=-1, @REF_DATE=@BDATE, @RET_DATE=@BDATE OUTPUT

    UPDATE #RESULT
       SET bdate = @BDATE
     WHERE adate = (SELECT MIN(adate) FROM #RESULT WHERE bdate IS NULL)
  END
END

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (1)'
  SELECT * FROM #RESULT ORDER BY bdate
END

CREATE TABLE #TEMP ( bdate datetime NOT NULL )
INSERT #TEMP SELECT DISTINCT bdate FROM #RESULT
TRUNCATE TABLE #RESULT
INSERT #RESULT (bdate, held, acct_bmk_wgt) SELECT bdate, 0, 0.0 FROM #TEMP
DROP TABLE #TEMP

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (2)'
  SELECT * FROM #RESULT ORDER BY bdate
END

UPDATE #RESULT
   SET held = 1
  FROM equity_common..position p
 WHERE #RESULT.bdate = p.reference_date
   AND p.reference_date = p.effective_date
   AND p.acct_cd IN (SELECT acct_cd FROM equity_common..account WHERE parent = @ACCOUNT_CD
                     UNION
                     SELECT acct_cd FROM equity_common..account WHERE acct_cd = @ACCOUNT_CD)
   AND p.security_id = @SECURITY_ID

IF EXISTS (SELECT 1 FROM #RESULT WHERE held = 1)
BEGIN
  CREATE TABLE #POSITION (
    bdate			datetime	NULL,
    security_id		int			NULL,
    units			float		NULL,
    price			float		NULL,
    mval			float		NULL,
    acct_wgt		float		NULL
  )

  INSERT #POSITION (bdate, security_id, units, price, acct_wgt)
  SELECT p.reference_date, p.security_id, SUM(ISNULL(p.quantity,0.0)), 0.0, 0.0
    FROM #RESULT r, equity_common..position p
   WHERE r.held = 1
     AND r.bdate = p.reference_date
     AND p.reference_date = p.effective_date
     AND p.acct_cd IN (SELECT acct_cd FROM equity_common..account WHERE parent = @ACCOUNT_CD
                       UNION
                       SELECT acct_cd FROM equity_common..account WHERE acct_cd = @ACCOUNT_CD)
   GROUP BY p.reference_date, p.security_id

  UPDATE #POSITION
     SET price = ISNULL(p.price_close_usd,0.0)
    FROM equity_common..market_price p
   WHERE #POSITION.bdate = p.reference_date
     AND #POSITION.security_id = p.security_id

  UPDATE #POSITION
     SET mval = units * price

  IF @DEBUG = 1
  BEGIN
    SELECT '#POSITION (1)'
    SELECT * FROM #POSITION ORDER BY bdate, security_id
  END

  UPDATE #POSITION
     SET acct_wgt = mval / x.tot_mval
    FROM (SELECT bdate, SUM(mval) AS tot_mval
            FROM #POSITION GROUP BY bdate) x
   WHERE #POSITION.bdate = x.bdate
     AND x.tot_mval != 0.0

  UPDATE #RESULT
     SET acct_bmk_wgt = p.acct_wgt
    FROM #POSITION p
   WHERE #RESULT.bdate = p.bdate
     AND p.security_id = @SECURITY_ID

  IF @DEBUG = 1
  BEGIN
    SELECT '#POSITION (2)'
    SELECT * FROM #POSITION ORDER BY bdate, security_id
    SELECT '#RESULT (3)'
    SELECT * FROM #RESULT ORDER BY bdate
  END

  DROP TABLE #POSITION
END

IF EXISTS (SELECT 1 FROM account a, benchmark b
            WHERE a.strategy_id = @STRATEGY_ID
              AND a.account_cd = @ACCOUNT_CD
              AND a.benchmark_cd = b.benchmark_cd)
BEGIN
  UPDATE #RESULT
     SET acct_bmk_wgt = acct_bmk_wgt - w.weight
    FROM account a, equity_common..benchmark_weight w
   WHERE a.strategy_id = @STRATEGY_ID
     AND a.account_cd = @ACCOUNT_CD
     AND a.benchmark_cd = w.acct_cd
     AND #RESULT.bdate = w.reference_date
     AND w.reference_date = w.effective_date
     AND w.security_id = @SECURITY_ID
END
ELSE
BEGIN
  UPDATE #RESULT
     SET acct_bmk_wgt = acct_bmk_wgt - (p.weight/100.0)
    FROM account a, universe_def d, universe_makeup p
   WHERE a.strategy_id = @STRATEGY_ID
     AND a.account_cd = @ACCOUNT_CD
     AND a.benchmark_cd = d.universe_cd
     AND d.universe_id = p.universe_id
     AND #RESULT.bdate = p.universe_dt
     AND p.security_id = @SECURITY_ID
END

UPDATE #RESULT
   SET total_score = s.total_score,
       universe_score = s.universe_score,
       region_score = s.region_score,
       country_score = s.country_score,
       ss_score = s.ss_score,
       sector_score = s.sector_score,
       segment_score = s.segment_score
  FROM scores s
 WHERE s.strategy_id = @STRATEGY_ID
   AND #RESULT.bdate = s.bdate
   AND s.security_id = @SECURITY_ID

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (4)'
  SELECT * FROM #RESULT ORDER BY bdate
END

DECLARE @PRECALC bit,
        @NUM int,
        @CATEGORY varchar(64)

SELECT @PRECALC = 0

IF EXISTS (SELECT 1 FROM category_score
            WHERE bdate IN (SELECT bdate FROM #RESULT)
              AND strategy_id = @STRATEGY_ID
              AND score_level = 'T'
              AND security_id = @SECURITY_ID)
  BEGIN SELECT @PRECALC = 1 END

CREATE TABLE #FACTOR_CATEGORY (
  ordinal		int identity(1,1)	NOT NULL,
  category_cd	varchar(1)			NOT NULL,
  category_nm	varchar(64)			NOT NULL
)

IF @PRECALC = 1
BEGIN
  INSERT #FACTOR_CATEGORY (category_cd, category_nm)
  SELECT code, decode FROM decode
   WHERE item = 'FACTOR_CATEGORY'
     AND code IN (SELECT DISTINCT category FROM category_score
                   WHERE bdate IN (SELECT bdate FROM #RESULT)
                     AND strategy_id = @STRATEGY_ID
                     AND score_level = 'T'
                     AND security_id = @SECURITY_ID)
  ORDER BY decode

  IF @DEBUG = 1
  BEGIN
    SELECT '#FACTOR_CATEGORY (PRE-CALC)'
    SELECT * FROM #FACTOR_CATEGORY ORDER BY ordinal
  END

  SELECT @NUM=0
  WHILE EXISTS (SELECT * FROM #FACTOR_CATEGORY WHERE ordinal > @NUM)
  BEGIN
    SELECT @NUM = MIN(ordinal) FROM #FACTOR_CATEGORY WHERE ordinal > @NUM
    SELECT @CATEGORY = category_nm FROM #FACTOR_CATEGORY WHERE ordinal = @NUM

    SELECT @SQL = 'ALTER TABLE #RESULT ADD [' + @CATEGORY + '] float NULL'
    IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
    EXEC(@SQL)

    SELECT @SQL = 'UPDATE #RESULT SET [' + @CATEGORY + '] = s.category_score '
    SELECT @SQL = @SQL + 'FROM #FACTOR_CATEGORY f, category_score s '
    SELECT @SQL = @SQL + 'WHERE #RESULT.bdate = s.bdate '
    SELECT @SQL = @SQL + 'AND s.strategy_id = ' + CONVERT(varchar,@STRATEGY_ID) + ' '
    SELECT @SQL = @SQL + 'AND s.security_id = ' + CONVERT(varchar,@SECURITY_ID) + ' '
    SELECT @SQL = @SQL + 'AND s.score_level = ''T'' '
    SELECT @SQL = @SQL + 'AND s.category = f.category_cd '
    SELECT @SQL = @SQL + 'AND f.category_nm = ''' + @CATEGORY + ''''
    IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
    EXEC(@SQL)
  END
END
ELSE
BEGIN
  CREATE TABLE #RANK (
    bdate			datetime	NULL,
    rank_event_id	int			NULL,
    against			varchar(1)	NULL,
    category_cd		varchar(1)	NULL,
    category_nm		varchar(64)	NULL,

    factor_id		int			NULL,
    factor_cd		varchar(32)	NULL,
    factor_short_nm	varchar(64)	NULL,
    factor_nm		varchar(255) NULL,

    weight1			float		NULL,
    weight2			float		NULL,
    weight3			float		NULL,
    rank			int			NULL,
    weighted_rank	float		NULL
  )

  INSERT #RANK (bdate, rank_event_id, against, factor_id, weight1, weight3, rank)
  SELECT i.bdate, i.rank_event_id, i.against, i.factor_id, w.weight, 0.0, o.rank
    FROM strategy g, factor_against_weight w, rank_inputs i, rank_output o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND i.bdate IN (SELECT bdate FROM #RESULT)
     AND i.universe_id = g.universe_id
     AND i.factor_id = w.factor_id
     AND i.against = w.against
     AND ISNULL(i.against_id,-9999) = ISNULL(w.against_id,-9999)
     AND i.rank_event_id = o.rank_event_id
     AND o.security_id = @SECURITY_ID

  IF @DEBUG = 1
  BEGIN
    SELECT '#RANK (1)'
    SELECT * FROM #RANK ORDER BY bdate, against, factor_id
  END

  --OVERRIDE WEIGHT LOGIC: BEGIN
  IF EXISTS (SELECT 1 FROM strategy g, factor_against_weight_override o
              WHERE g.strategy_id = @STRATEGY_ID AND g.factor_model_id = o.factor_model_id)
  BEGIN
    UPDATE #RANK
       SET weight1 = o.override_wgt
      FROM #SECURITY y, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RANK.factor_id = o.factor_id
       AND #RANK.against = o.against
       AND #RANK.against = 'U'
       AND o.level_type = 'G'
       AND o.level_id = y.segment_id
    UPDATE #RANK
       SET weight1 = o.override_wgt
      FROM #SECURITY y, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RANK.factor_id = o.factor_id
       AND #RANK.against = o.against
       AND #RANK.against = 'C'
       AND ISNULL(y.sector_id, -9999) = ISNULL(o.against_id, -9999)
       AND o.level_type = 'G'
       AND o.level_id = y.segment_id
    UPDATE #RANK
       SET weight1 = o.override_wgt
      FROM #SECURITY y, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RANK.factor_id = o.factor_id
       AND #RANK.against = o.against
       AND #RANK.against = 'G'
       AND ISNULL(y.segment_id, -9999) = ISNULL(o.against_id, -9999)
       AND o.level_type = 'G'
       AND o.level_id = y.segment_id

    UPDATE #RANK
       SET weight1 = o.override_wgt
      FROM #SECURITY y, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RANK.factor_id = o.factor_id
       AND #RANK.against = o.against
       AND #RANK.against = 'U'
       AND o.level_type = 'C'
       AND o.level_id = y.sector_id
    UPDATE #RANK
       SET weight1 = o.override_wgt
      FROM #SECURITY y, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RANK.factor_id = o.factor_id
       AND #RANK.against = o.against
       AND #RANK.against = 'C'
       AND ISNULL(y.sector_id, -9999) = ISNULL(o.against_id, -9999)
       AND o.level_type = 'C'
       AND o.level_id = y.sector_id
    UPDATE #RANK
       SET weight1 = o.override_wgt
      FROM #SECURITY y, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RANK.factor_id = o.factor_id
       AND #RANK.against = o.against
       AND #RANK.against = 'G'
       AND ISNULL(y.segment_id, -9999) = ISNULL(o.against_id, -9999)
       AND o.level_type = 'C'
       AND o.level_id = y.sector_id

    UPDATE #RANK
       SET weight1 = o.override_wgt
      FROM #SECURITY y, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RANK.factor_id = o.factor_id
       AND #RANK.against = o.against
       AND #RANK.against = 'U'
       AND o.level_type = 'U'
    UPDATE #RANK
       SET weight1 = o.override_wgt
      FROM #SECURITY y, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RANK.factor_id = o.factor_id
       AND #RANK.against = 'C'
       AND ISNULL(y.sector_id, -9999) = ISNULL(o.against_id, -9999)
       AND o.level_type = 'U'
    UPDATE #RANK
       SET weight1 = o.override_wgt
      FROM #SECURITY y, strategy g, factor_against_weight_override o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = o.factor_model_id
       AND #RANK.factor_id = o.factor_id
       AND #RANK.against = o.against
       AND #RANK.against = 'G'
       AND ISNULL(y.segment_id, -9999) = ISNULL(o.against_id, -9999)
       AND o.level_type = 'U'
  END
  --OVERRIDE WEIGHT LOGIC: END

  /*
  NOTE: CURRENTLY NO CODE FOR WEIGHT OVERRIDES INVOLVING COUNTRY OR REGION;
        WOULD REQUIRE ADDING COLUMN level_cd TO TABLE factor_against_weight_override
  */

  IF @DEBUG = 1
  BEGIN
    SELECT '#RANK (2)'
    SELECT * FROM #RANK ORDER BY against, factor_id
  END

  DELETE #RANK WHERE weight1 = 0.0

  UPDATE #RANK
     SET factor_cd = f.factor_cd,
         factor_short_nm = f.factor_short_nm,
         factor_nm = f.factor_nm
    FROM factor f
   WHERE #RANK.factor_id = f.factor_id

  UPDATE #RANK
     SET category_cd = d.code,
         category_nm = d.decode
    FROM strategy g, factor_category c, decode d
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = c.factor_model_id
     AND #RANK.factor_id = c.factor_id
     AND d.item = 'FACTOR_CATEGORY'
     AND d.code = c.category

  IF @DEBUG = 1
  BEGIN
    SELECT '#RANK (3)'
    SELECT * FROM #RANK ORDER BY against, factor_id
  END

  UPDATE #RANK
     SET weight2 = weight1 * w.universe_total_wgt
    FROM #SECURITY s, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND ISNULL(s.sector_id,-9999) = ISNULL(w.sector_id,-9999)
     AND ISNULL(s.segment_id,-9999) = ISNULL(w.segment_id,-9999)
     AND #RANK.against = 'U'
  UPDATE #RANK --SEGMENT MAY EXIST SUCH AS WITH STANDARD GICS SECTOR MODEL BUT NOT UTILIZED
     SET weight2 = weight1 * w.universe_total_wgt
    FROM #SECURITY s, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND ISNULL(s.sector_id,-9999) = ISNULL(w.sector_id,-9999)
     AND #RANK.against = 'U'
     AND #RANK.weight2 IS NULL

  UPDATE #RANK
     SET weight2 = weight1 * w.sector_ss_wgt * w.ss_total_wgt
    FROM #SECURITY s, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND ISNULL(s.sector_id,-9999) = ISNULL(w.sector_id,-9999)
     AND ISNULL(s.segment_id,-9999) = ISNULL(w.segment_id,-9999)
     AND #RANK.against = 'C'
  UPDATE #RANK --SAME AS PREVIOUS COMMENT
     SET weight2 = weight1 * w.sector_ss_wgt * w.ss_total_wgt
    FROM #SECURITY s, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND ISNULL(s.sector_id,-9999) = ISNULL(w.sector_id,-9999)
     AND #RANK.against = 'C'
     AND #RANK.weight2 IS NULL

  UPDATE #RANK
     SET weight2 = weight1 * w.segment_ss_wgt * w.ss_total_wgt
    FROM #SECURITY s, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND ISNULL(s.sector_id,-9999) = ISNULL(w.sector_id,-9999)
     AND ISNULL(s.segment_id,-9999) = ISNULL(w.segment_id,-9999)
     AND #RANK.against = 'G'
  UPDATE #RANK --SAME AS PREVIOUS COMMENT
     SET weight2 = weight1 * w.segment_ss_wgt * w.ss_total_wgt
    FROM #SECURITY s, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND ISNULL(s.sector_id,-9999) = ISNULL(w.sector_id,-9999)
     AND #RANK.against = 'G'
     and #RANK.weight2 IS NULL

  UPDATE #RANK
     SET weight2 = weight1 * w.region_total_wgt
    FROM #SECURITY s, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND ISNULL(s.sector_id,-9999) = ISNULL(w.sector_id,-9999)
     AND ISNULL(s.segment_id,-9999) = ISNULL(w.segment_id,-9999)
     AND #RANK.against = 'R'
  UPDATE #RANK --SAME AS PREVIOUS COMMENT
     SET weight2 = weight1 * w.region_total_wgt
    FROM #SECURITY s, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND ISNULL(s.sector_id,-9999) = ISNULL(w.sector_id,-9999)
     AND #RANK.against = 'R'
     and #RANK.weight2 IS NULL

  UPDATE #RANK
     SET weight2 = weight1 * w.country_total_wgt
    FROM #SECURITY s, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND ISNULL(s.sector_id,-9999) = ISNULL(w.sector_id,-9999)
     AND ISNULL(s.segment_id,-9999) = ISNULL(w.segment_id,-9999)
     AND #RANK.against = 'Y'
  UPDATE #RANK --SAME AS PREVIOUS COMMENT
     SET weight2 = weight1 * w.country_total_wgt
    FROM #SECURITY s, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND ISNULL(s.sector_id,-9999) = ISNULL(w.sector_id,-9999)
     AND #RANK.against = 'Y'
     and #RANK.weight2 IS NULL

  UPDATE #RANK
     SET weight3 = weight2 / sum_weight2
    FROM (SELECT bdate, category_cd, SUM(weight2) AS sum_weight2
            FROM #RANK GROUP BY bdate, category_cd) x
   WHERE #RANK.bdate = x.bdate
     AND #RANK.category_cd = x.category_cd
     AND x.sum_weight2 != 0.0

  UPDATE #RANK
     SET weighted_rank = weight3 * rank

  IF @DEBUG = 1
  BEGIN
    SELECT '#RANK (4)'
    SELECT * FROM #RANK ORDER BY against, factor_id
  END

  INSERT #FACTOR_CATEGORY (category_cd, category_nm)
  SELECT DISTINCT d.code, d.decode
    FROM #RANK r, decode d
   WHERE d.item = 'FACTOR_CATEGORY'
     AND r.category_cd = d.code
   ORDER BY d.decode

  IF @DEBUG = 1
  BEGIN
    SELECT '#FACTOR_CATEGORY (ON-THE-FLY)'
    SELECT * FROM #FACTOR_CATEGORY ORDER BY ordinal
  END

  SELECT @NUM=0
  WHILE EXISTS (SELECT * FROM #FACTOR_CATEGORY WHERE ordinal > @NUM)
  BEGIN
    SELECT @NUM = MIN(ordinal) FROM #FACTOR_CATEGORY WHERE ordinal > @NUM
    SELECT @CATEGORY = category_nm FROM #FACTOR_CATEGORY WHERE ordinal = @NUM

    SELECT @SQL = 'ALTER TABLE #RESULT ADD [' + @CATEGORY + '] float NULL'
    IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
    EXEC(@SQL)

    SELECT @SQL = 'UPDATE #RESULT SET [' + @CATEGORY + '] = x.sum_weighted_rank '
    SELECT @SQL = @SQL + 'FROM (SELECT bdate, SUM(weighted_rank) AS [sum_weighted_rank] '
    SELECT @SQL = @SQL + 'FROM #RANK WHERE category_nm = ''' + @CATEGORY + ''' GROUP BY bdate) x '
    SELECT @SQL = @SQL + 'WHERE #RESULT.bdate = x.bdate'
    IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
    EXEC(@SQL)
  END

  DROP TABLE #RANK
END

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (5)'
  SELECT * FROM #RESULT ORDER BY bdate
END

SELECT @SQL = 'SELECT bdate AS [Date], '
SELECT @SQL = @SQL + 'acct_bmk_wgt AS [Acct-Bmk Wgt], '
SELECT @SQL = @SQL + 'ROUND(total_score,1) AS [Total], '
SELECT @SQL = @SQL + 'ROUND(universe_score,1) AS [Universe], '
SELECT @SQL = @SQL + 'ROUND(region_score,1) AS [Region], '
SELECT @SQL = @SQL + 'ROUND(country_score,1) AS [Country], '
SELECT @SQL = @SQL + 'ROUND(ss_score,1) AS [SS], '
SELECT @SQL = @SQL + 'ROUND(sector_score,1) AS [Sector], '
SELECT @SQL = @SQL + 'ROUND(segment_score,1) AS [Segment]'

SELECT @NUM=0
WHILE EXISTS (SELECT * FROM #FACTOR_CATEGORY WHERE ordinal > @NUM)
BEGIN
  SELECT @NUM = MIN(ordinal) FROM #FACTOR_CATEGORY WHERE ordinal > @NUM
  SELECT @CATEGORY = category_nm FROM #FACTOR_CATEGORY WHERE ordinal = @NUM
  SELECT @SQL = @SQL + ', [' + @CATEGORY + ']'
END

SELECT @SQL = @SQL + ' FROM #RESULT ORDER BY bdate DESC'
IF @DEBUG = 1 BEGIN SELECT '@SQL', @SQL END
EXEC(@SQL)

DROP TABLE #FACTOR_CATEGORY
DROP TABLE #SECURITY
DROP TABLE #RESULT

RETURN 0
go
IF OBJECT_ID('dbo.rpt_stock_view_history') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_stock_view_history >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_stock_view_history >>>'
go
