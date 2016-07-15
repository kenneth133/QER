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
CREATE PROCEDURE dbo.rpt_stock_view_history @BDATE datetime,
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
IF @IDENTIFIER_TYPE NOT IN ('TICKER', 'CUSIP', 'SEDOL')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @IDENTIFIER_TYPE PARAMETER' RETURN -1 END
IF @IDENTIFIER_VALUE IS NULL
  BEGIN SELECT 'ERROR: @IDENTIFIER_VALUE IS A REQUIRED PARAMETER' RETURN -1 END

CREATE TABLE #RESULT (
  adate				datetime	NULL,
  bdate				datetime	NULL,
  acct_bmk_wgt		float		NULL,
  total_score		float		NULL,
  universe_score	float		NULL,
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

WHILE EXISTS (SELECT * FROM #RESULT WHERE bdate IS NULL)
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
DELETE #RESULT
INSERT #RESULT (bdate) SELECT bdate FROM #TEMP
DROP TABLE #TEMP

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (2)'
  SELECT * FROM #RESULT ORDER BY bdate
END

CREATE TABLE #SECURITY (
  mqa_id	varchar(32)	NULL,
  ticker	varchar(16)	NULL,
  cusip		varchar(32)	NULL,
  sedol		varchar(32)	NULL,
  isin		varchar(64)	NULL,
  imnt_nm	varchar(255)	NULL,

  country_cd	varchar(4)	NULL,
  country_nm	varchar(128)	NULL,
  sector_id	int		NULL,
  sector_nm	varchar(64)	NULL,
  segment_id	int		NULL,
  segment_nm	varchar(128)	NULL
)

IF @IDENTIFIER_TYPE = 'TICKER'
BEGIN
  INSERT #SECURITY
        (mqa_id, ticker, cusip, sedol, isin, imnt_nm, country_cd)
  SELECT i.mqa_id, i.ticker, i.cusip, i.sedol, i.isin, i.imnt_nm, i.country
    FROM instrument_characteristics i
   WHERE i.bdate = @BDATE
     AND i.ticker = @IDENTIFIER_VALUE
END
ELSE IF @IDENTIFIER_TYPE = 'CUSIP'
BEGIN
  INSERT #SECURITY
        (mqa_id, ticker, cusip, sedol, isin, imnt_nm, country_cd)
  SELECT i.mqa_id, i.ticker, i.cusip, i.sedol, i.isin, i.imnt_nm, i.country
    FROM instrument_characteristics i
   WHERE i.bdate = @BDATE
     AND i.cusip = @IDENTIFIER_VALUE
END
ELSE IF @IDENTIFIER_TYPE = 'SEDOL'
BEGIN
  INSERT #SECURITY
        (mqa_id, ticker, cusip, sedol, isin, imnt_nm, country_cd)
  SELECT i.mqa_id, i.ticker, i.cusip, i.sedol, i.isin, i.imnt_nm, i.country
    FROM instrument_characteristics i
   WHERE i.bdate = @BDATE
     AND i.sedol = @IDENTIFIER_VALUE
END

UPDATE #SECURITY
   SET country_nm = d.decode
  FROM decode d
 WHERE d.item = 'COUNTRY'
   AND #SECURITY.country_cd = d.code

IF @IDENTIFIER_TYPE IN ('TICKER', 'CUSIP')
BEGIN
  UPDATE #SECURITY
     SET sector_id = ss.sector_id,
         segment_id = ss.segment_id
    FROM sector_model_security ss, strategy g, factor_model f
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = f.factor_model_id
     AND ss.bdate = @BDATE
     AND ss.sector_model_id = f.sector_model_id
     AND #SECURITY.cusip = ss.cusip
END
ELSE IF @IDENTIFIER_TYPE = 'SEDOL'
BEGIN
  UPDATE #SECURITY
     SET sector_id = ss.sector_id,
         segment_id = ss.segment_id
    FROM sector_model_security ss, strategy g, factor_model f
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = f.factor_model_id
     AND ss.bdate = @BDATE
     AND ss.sector_model_id = f.sector_model_id
     AND #SECURITY.sedol = ss.sedol
END

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
  SELECT '#SECURITY'
  SELECT * FROM #SECURITY ORDER BY cusip, sedol
END

SELECT ticker		AS [Ticker],
       cusip		AS [CUSIP],
       sedol		AS [SEDOL],
       isin			AS [ISIN],
       imnt_nm		AS [Name],
       country_nm	AS [Country Name],
       ISNULL(sector_nm, 'UNKNOWN') AS [Sector Name],
       ISNULL(segment_nm, 'UNKNOWN') AS [Segment Name]
  FROM #SECURITY

CREATE TABLE #POSITION (
  bdate		datetime	NULL,
  cusip		varchar(32)	NULL,
  sedol		varchar(32)	NULL,

  units		float		NULL,
  price		float		NULL,
  mval		float		NULL,
  
  account_wgt	float	NULL,
  benchmark_wgt	float	NULL,
  acct_bmk_wgt	float	NULL
)

INSERT #POSITION (bdate, cusip, sedol, units)
SELECT p.bdate, p.cusip, p.sedol, ISNULL(p.units, 0.0)
  FROM #RESULT r, position p
 WHERE r.bdate = p.bdate
   AND p.account_cd = @ACCOUNT_CD

IF @IDENTIFIER_TYPE IN ('TICKER', 'CUSIP')
BEGIN
  UPDATE #POSITION
     SET price = i.price_close
    FROM instrument_characteristics i
   WHERE #POSITION.bdate = i.bdate
     AND #POSITION.cusip = i.cusip
END
ELSE IF @IDENTIFIER_TYPE = 'SEDOL'
BEGIN
  UPDATE #POSITION
     SET price = i.price_close
    FROM instrument_characteristics i
   WHERE #POSITION.bdate = i.bdate
     AND #POSITION.sedol = i.sedol
END

UPDATE #POSITION
   SET price = 1.0
 WHERE cusip = '_USD'

UPDATE #POSITION
   SET mval = units * price

UPDATE #POSITION
   SET account_wgt = mval / x.tot_mval
  FROM (SELECT bdate, SUM(mval) AS tot_mval
          FROM #POSITION GROUP BY bdate) x
 WHERE #POSITION.bdate = x.bdate

IF @DEBUG = 1
BEGIN
  SELECT '#POSITION (1)'
  SELECT * FROM #POSITION ORDER BY bdate, cusip, sedol
END

IF @IDENTIFIER_TYPE IN ('TICKER', 'CUSIP')
BEGIN
  DELETE #POSITION
   WHERE cusip NOT IN (SELECT cusip FROM #SECURITY)
END
ELSE IF @IDENTIFIER_TYPE = 'SEDOL'
BEGIN
  DELETE #POSITION
   WHERE sedol NOT IN (SELECT sedol FROM #SECURITY)
END

INSERT #POSITION (bdate, cusip, sedol, account_wgt)
SELECT r.bdate, s.cusip, s.sedol, 0.0
  FROM #SECURITY s, #RESULT r
 WHERE r.bdate NOT IN (SELECT bdate FROM #POSITION)

IF @IDENTIFIER_TYPE IN ('TICKER', 'CUSIP')
BEGIN
  UPDATE #POSITION
     SET benchmark_wgt = p.weight / 100.0
    FROM account a, universe_makeup p
   WHERE a.strategy_id = @STRATEGY_ID
     AND a.account_cd = @ACCOUNT_CD
     AND a.bm_universe_id = p.universe_id
     AND #POSITION.bdate = p.universe_dt
     AND #POSITION.cusip = p.cusip
END
ELSE IF @IDENTIFIER_TYPE = 'SEDOL'
BEGIN
  UPDATE #POSITION
     SET benchmark_wgt = p.weight / 100.0
    FROM account a, universe_makeup p
   WHERE a.strategy_id = @STRATEGY_ID
     AND a.account_cd = @ACCOUNT_CD
     AND a.bm_universe_id = p.universe_id
     AND #POSITION.bdate = p.universe_dt
     AND #POSITION.sedol = p.sedol
END

UPDATE #POSITION
   SET benchmark_wgt = 0.0
 WHERE benchmark_wgt IS NULL

UPDATE #POSITION
   SET acct_bmk_wgt = account_wgt - benchmark_wgt

IF @DEBUG = 1
BEGIN
  SELECT '#POSITION (2)'
  SELECT * FROM #POSITION ORDER BY bdate, cusip, sedol
END

UPDATE #RESULT
   SET acct_bmk_wgt = p.acct_bmk_wgt
  FROM #POSITION p
 WHERE #RESULT.bdate = p.bdate

DROP TABLE #POSITION

IF @IDENTIFIER_TYPE IN ('TICKER', 'CUSIP')
BEGIN
  UPDATE #RESULT
     SET total_score = s.total_score,
         universe_score = s.universe_score,
         sector_score = s.sector_score,
         segment_score = s.segment_score
    FROM scores s
   WHERE s.strategy_id = @STRATEGY_ID
     AND s.bdate = #RESULT.bdate
     AND s.cusip IN (SELECT cusip FROM #SECURITY)
END
ELSE IF @IDENTIFIER_TYPE = 'SEDOL'
BEGIN
  UPDATE #RESULT
     SET total_score = s.total_score,
         universe_score = s.universe_score,
         sector_score = s.sector_score,
         segment_score = s.segment_score
    FROM scores s
   WHERE s.strategy_id = @STRATEGY_ID
     AND s.bdate = #RESULT.bdate
     AND s.sedol IN (SELECT sedol FROM #SECURITY)
END

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (3)'
  SELECT * FROM #RESULT ORDER BY bdate
END

DECLARE @PRECALC bit,
        @NUM int,
        @CATEGORY varchar(64),
        @SQL varchar(1000)

SELECT @PRECALC = 0

IF @IDENTIFIER_TYPE IN ('TICKER', 'CUSIP')
BEGIN
  IF EXISTS (SELECT * FROM category_score
              WHERE bdate IN (SELECT DISTINCT bdate FROM #RESULT)
                AND strategy_id = @STRATEGY_ID
                AND score_level = 'T'
                AND cusip IN (SELECT cusip FROM #SECURITY))
    BEGIN SELECT @PRECALC = 1 END
END
ELSE IF @IDENTIFIER_TYPE = 'SEDOL'
BEGIN
  IF EXISTS (SELECT * FROM category_score
              WHERE bdate IN (SELECT DISTINCT bdate FROM #RESULT)
                AND strategy_id = @STRATEGY_ID
                AND score_level = 'T'
                AND sedol IN (SELECT sedol FROM #SECURITY))
    BEGIN SELECT @PRECALC = 1 END
END

CREATE TABLE #FACTOR_CATEGORY (
  ordinal		int identity(1,1)	NOT NULL,
  category_cd	varchar(1)			NOT NULL,
  category_nm	varchar(64)			NOT NULL
)

IF @PRECALC = 1
BEGIN
  IF @IDENTIFIER_TYPE IN ('TICKER', 'CUSIP')
  BEGIN
    INSERT #FACTOR_CATEGORY (category_cd, category_nm)
    SELECT code, decode FROM decode
     WHERE item = 'FACTOR_CATEGORY'
       AND code IN (SELECT DISTINCT category FROM category_score
                     WHERE bdate IN (SELECT DISTINCT bdate FROM #RESULT)
                       AND strategy_id = @STRATEGY_ID
                       AND score_level = 'T'
                       AND cusip IN (SELECT cusip FROM #SECURITY))
    ORDER BY decode
  END
  ELSE IF @IDENTIFIER_TYPE = 'SEDOL'
  BEGIN
    INSERT #FACTOR_CATEGORY (category_cd, category_nm)
    SELECT code, decode FROM decode
     WHERE item = 'FACTOR_CATEGORY'
       AND code IN (SELECT DISTINCT category FROM category_score
                     WHERE bdate IN (SELECT DISTINCT bdate FROM #RESULT)
                       AND strategy_id = @STRATEGY_ID
                       AND score_level = 'T'
                       AND sedol IN (SELECT sedol FROM #SECURITY))
    ORDER BY decode
  END

  IF @DEBUG = 1
  BEGIN
    SELECT '#FACTOR_CATEGORY (PRE-CALC)'
    SELECT * FROM #FACTOR_CATEGORY ORDER BY ordinal
  END

  SELECT @NUM=0
  WHILE EXISTS (SELECT * FROM #FACTOR_CATEGORY WHERE ordinal > @NUM)
  BEGIN
    SELECT @NUM = MIN(ordinal)
      FROM #FACTOR_CATEGORY
     WHERE ordinal > @NUM

    SELECT @CATEGORY = category_nm
      FROM #FACTOR_CATEGORY
     WHERE ordinal = @NUM

    SELECT @SQL = 'ALTER TABLE #RESULT ADD [' + @CATEGORY + '] float NULL'
    IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
    EXEC(@SQL)

    SELECT @SQL = 'UPDATE #RESULT SET [' + @CATEGORY + '] = s.category_score '
    SELECT @SQL = @SQL + 'FROM #FACTOR_CATEGORY f, category_score s '
    SELECT @SQL = @SQL + 'WHERE #RESULT.bdate = s.bdate '
    SELECT @SQL = @SQL + 'AND s.strategy_id = ' + CONVERT(varchar,@STRATEGY_ID) + ' '
    IF @IDENTIFIER_TYPE IN ('TICKER', 'CUSIP')
      BEGIN SELECT @SQL = @SQL + 'AND s.cusip IN (SELECT cusip FROM #SECURITY) ' END
    ELSE IF @IDENTIFIER_TYPE = 'SEDOL'
      BEGIN SELECT @SQL = @SQL + 'AND s.sedol IN (SELECT sedol FROM #SECURITY) ' END
    SELECT @SQL = @SQL + 'AND s.score_level = ''T'' '
    SELECT @SQL = @SQL + 'AND s.category = f.category_cd '
    SELECT @SQL = @SQL + 'AND f.category_nm = ''' + @CATEGORY + ''''
    IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
    EXEC(@SQL)
  END
END
ELSE
BEGIN
  CREATE TABLE #RESULT2 (
    bdate		datetime	NULL,
    category_cd	varchar(1)	NULL,
    category_nm	varchar(64)	NULL,

    factor_id	int				NULL,
    factor_cd	varchar(64)		NULL,
    factor_nm	varchar(255)	NULL,

    against			varchar(1)	NULL,
    against_id		int			NULL,
    weight1			float		NULL,
    weight2			float		NULL,
    weight3			float		NULL,
    rank			int			NULL,
    weighted_rank	float		NULL
  )

  IF @IDENTIFIER_TYPE IN ('TICKER', 'CUSIP')
  BEGIN
    INSERT #RESULT2 (bdate, factor_id, factor_cd, factor_nm, against, against_id, weight1, rank)
    SELECT r.bdate, f.factor_id, f.factor_cd, f.factor_nm, w.against, w.against_id, w.weight, o.rank
      FROM #RESULT r, #SECURITY s, strategy g, factor f, factor_against_weight w, rank_inputs i, rank_output o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = w.factor_model_id
       AND w.factor_id = f.factor_id
       AND i.bdate = r.bdate
       AND i.universe_id = g.universe_id
       AND i.factor_id = w.factor_id
       AND i.against = w.against
       AND (i.against_id = w.against_id OR (i.against_id IS NULL AND w.against_id IS NULL))
       AND i.rank_event_id = o.rank_event_id
       AND s.cusip = o.cusip
  END
  ELSE IF @IDENTIFIER_TYPE = 'SEDOL'
  BEGIN
    INSERT #RESULT2 (bdate, factor_id, factor_cd, factor_nm, against, against_id, weight1, rank)
    SELECT r.bdate, f.factor_id, f.factor_cd, f.factor_nm, w.against, w.against_id, w.weight, o.rank
      FROM #RESULT r, #SECURITY s, strategy g, factor f, factor_against_weight w, rank_inputs i, rank_output o
     WHERE g.strategy_id = @STRATEGY_ID
       AND g.factor_model_id = w.factor_model_id
       AND w.factor_id = f.factor_id
       AND i.bdate = r.bdate
       AND i.universe_id = g.universe_id
       AND i.factor_id = w.factor_id
       AND i.against = w.against
       AND (i.against_id = w.against_id OR (i.against_id IS NULL AND w.against_id IS NULL))
       AND i.rank_event_id = o.rank_event_id
       AND s.sedol = o.sedol
  END

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT2 (1)'
    SELECT * FROM #RESULT2 ORDER BY bdate, against, against_id
  END

  --OVERRIDE WEIGHT LOGIC: BEGIN
  UPDATE #RESULT2
     SET weight1 = o.override_wgt
    FROM strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RESULT2.factor_id = o.factor_id
     AND #RESULT2.against = o.against
     AND (#RESULT2.against_id = o.against_id OR (#RESULT2.against_id IS NULL AND o.against_id IS NULL))
     AND o.level_type = 'G'
     AND o.level_id IN (SELECT segment_id FROM #SECURITY)
  UPDATE #RESULT2
     SET weight1 = o.override_wgt
    FROM strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RESULT2.factor_id = o.factor_id
     AND #RESULT2.against = o.against
     AND (#RESULT2.against_id = o.against_id OR (#RESULT2.against_id IS NULL AND o.against_id IS NULL))
     AND o.level_type = 'C'
     AND o.level_id IN (SELECT sector_id FROM #SECURITY)
  UPDATE #RESULT2
     SET weight1 = o.override_wgt
    FROM strategy g, factor_against_weight_override o
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = o.factor_model_id
     AND #RESULT2.factor_id = o.factor_id
     AND #RESULT2.against = o.against
     AND (#RESULT2.against_id = o.against_id OR (#RESULT2.against_id IS NULL AND o.against_id IS NULL))
     AND o.level_type = 'U'
  --OVERRIDE WEIGHT LOGIC: END

  DELETE #RESULT2 WHERE weight1 = 0.0

  UPDATE #RESULT2
     SET weight2 = weight1 * w.universe_total_wgt
    FROM #SECURITY s, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND (s.sector_id = w.sector_id OR (s.sector_id IS NULL AND w.sector_id IS NULL))
     AND (s.segment_id = w.segment_id OR (s.segment_id IS NULL AND w.segment_id IS NULL))
     AND #RESULT2.against = 'U'
  UPDATE #RESULT2
     SET weight2 = weight1 * w.sector_ss_wgt * ss_total_wgt
    FROM #SECURITY s, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND (s.sector_id = w.sector_id OR (s.sector_id IS NULL AND w.sector_id IS NULL))
     AND (s.segment_id = w.segment_id OR (s.segment_id IS NULL AND w.segment_id IS NULL))
     AND #RESULT2.against = 'C'
  UPDATE #RESULT2
     SET weight2 = weight1 * w.segment_ss_wgt * ss_total_wgt
    FROM #SECURITY s, strategy g, factor_model_weights w
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = w.factor_model_id
     AND (s.sector_id = w.sector_id OR (s.sector_id IS NULL AND w.sector_id IS NULL))
     AND (s.segment_id = w.segment_id OR (s.segment_id IS NULL AND w.segment_id IS NULL))
     AND #RESULT2.against = 'G'

  UPDATE #RESULT2
     SET category_cd = d.code, 
         category_nm = d.decode
    FROM strategy g, factor_category y, decode d
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.factor_model_id = y.factor_model_id
     AND #RESULT2.factor_id = y.factor_id
     AND d.item = 'FACTOR_CATEGORY'
     AND y.category = d.code

  UPDATE #RESULT2
     SET weight3 = weight2 / sum_weight2
    FROM (SELECT bdate, category_cd, SUM(weight2) AS sum_weight2
            FROM #RESULT2 GROUP BY bdate, category_cd) x
   WHERE #RESULT2.bdate = x.bdate
     AND #RESULT2.category_cd = x.category_cd

  UPDATE #RESULT2
     SET weighted_rank = weight3 * rank

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT2 (2)'
    SELECT * FROM #RESULT2 ORDER BY bdate, against, against_id
  END

  INSERT #FACTOR_CATEGORY (category_cd, category_nm)
  SELECT DISTINCT category_cd, category_nm
    FROM #RESULT2
   ORDER BY category_nm

  IF @DEBUG = 1
  BEGIN
    SELECT '#FACTOR_CATEGORY (ON-THE-FLY)'
    SELECT * FROM #FACTOR_CATEGORY ORDER BY ordinal
  END

  SELECT @NUM=0
  WHILE EXISTS (SELECT * FROM #FACTOR_CATEGORY WHERE ordinal > @NUM)
  BEGIN
    SELECT @NUM = MIN(ordinal)
      FROM #FACTOR_CATEGORY
     WHERE ordinal > @NUM

    SELECT @CATEGORY = category_nm
      FROM #FACTOR_CATEGORY
     WHERE ordinal = @NUM

    SELECT @SQL = 'ALTER TABLE #RESULT ADD [' + @CATEGORY + '] float NULL'
    IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
    EXEC(@SQL)

    SELECT @SQL = 'UPDATE #RESULT SET [' + @CATEGORY + '] = x.sum_weighted_rank '
    SELECT @SQL = @SQL + 'FROM (SELECT bdate, SUM(weighted_rank) AS [sum_weighted_rank] '
    SELECT @SQL = @SQL + 'FROM #RESULT2 WHERE category_nm = ''' + @CATEGORY + ''' GROUP BY bdate) x '
    SELECT @SQL = @SQL + 'WHERE #RESULT.bdate = x.bdate'
    IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
    EXEC(@SQL)
  END

  DROP TABLE #RESULT2
END

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (4)'
  SELECT * FROM #RESULT ORDER BY bdate
END

IF NOT EXISTS(SELECT * FROM #RESULT WHERE total_score IS NOT NULL)
  BEGIN DELETE #RESULT END

SELECT @SQL = 'SELECT bdate AS [Date], '
SELECT @SQL = @SQL + 'acct_bmk_wgt AS [Acct-Bmk Wgt], '
SELECT @SQL = @SQL + 'ROUND(total_score,1) AS [Total], '
SELECT @SQL = @SQL + 'ROUND(universe_score,1) AS [Universe], '
SELECT @SQL = @SQL + 'ROUND(sector_score,1) AS [Sector], '
SELECT @SQL = @SQL + 'ROUND(segment_score,1) AS [Segment]'

SELECT @NUM=0
WHILE EXISTS (SELECT * FROM #FACTOR_CATEGORY WHERE ordinal > @NUM)
BEGIN
  SELECT @NUM = MIN(ordinal)
    FROM #FACTOR_CATEGORY
   WHERE ordinal > @NUM

  SELECT @CATEGORY = category_nm
    FROM #FACTOR_CATEGORY
   WHERE ordinal = @NUM

  SELECT @SQL = @SQL + ', [' + @CATEGORY + ']'
END

SELECT @SQL = @SQL + 'FROM #RESULT ORDER BY bdate DESC'
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
