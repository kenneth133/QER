use QER
go
IF OBJECT_ID('dbo.universe_makeup_load') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.universe_makeup_load
    IF OBJECT_ID('dbo.universe_makeup_load') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.universe_makeup_load >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.universe_makeup_load >>>'
END
go
CREATE PROCEDURE dbo.universe_makeup_load
@SOURCE_CD varchar(8) = 'FS',
@DEBUG bit = NULL
AS

SELECT @SOURCE_CD = UPPER(@SOURCE_CD)

IF @SOURCE_CD IS NULL
  BEGIN SELECT 'ERROR: @SOURCE_CD IS A REQUIRED PARAMETER' RETURN -1 END
IF NOT EXISTS (SELECT * FROM decode WHERE item = 'SOURCE_CD' AND code = @SOURCE_CD)
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @SOURCE_CD PARAMETER' RETURN -1 END

IF @DEBUG = 1
BEGIN
  SELECT 'universe_makeup_staging (1)'
  SELECT * FROM universe_makeup_staging ORDER BY cusip, sedol, ticker, isin
END

DELETE universe_makeup_staging
 WHERE cusip IS NULL
   AND ticker IS NULL
   AND sedol IS NULL
   AND isin IS NULL

IF @DEBUG = 1
BEGIN
  SELECT 'universe_makeup_staging (2)'
  SELECT * FROM universe_makeup_staging ORDER BY cusip, sedol, ticker, isin
END

CREATE TABLE #UNIVERSE_MAKEUP_STAGING (
  universe_dt	datetime	NOT NULL,
  universe_cd	varchar(32)	NOT NULL,
  security_id	int			NULL,
  ticker		varchar(32)	NULL,
  cusip			varchar(32)	NULL,
  sedol			varchar(32)	NULL,
  isin			varchar(32)	NULL,
  currency_cd	varchar(3)	NULL,
  exchange_nm	varchar(60)	NULL,
  weight		float		NULL 
)

INSERT #UNIVERSE_MAKEUP_STAGING
SELECT universe_dt, universe_cd, NULL, ticker, cusip, sedol, isin, currency_cd, exchange_nm, weight
  FROM universe_makeup_staging

IF @DEBUG = 1
BEGIN
  SELECT '#UNIVERSE_MAKEUP_STAGING (1)'
  SELECT * FROM #UNIVERSE_MAKEUP_STAGING ORDER BY security_id, cusip, sedol, ticker, isin
END

EXEC security_id_update @TABLE_NAME='#UNIVERSE_MAKEUP_STAGING', @DATE_COL='universe_dt'

IF @DEBUG = 1
BEGIN
  SELECT '#UNIVERSE_MAKEUP_STAGING (2)'
  SELECT * FROM #UNIVERSE_MAKEUP_STAGING ORDER BY security_id, cusip, sedol, ticker, isin
END

IF EXISTS (SELECT 1 FROM #UNIVERSE_MAKEUP_STAGING WHERE security_id IS NULL)
BEGIN
  DECLARE @DATE datetime
  SELECT @DATE = '1/1/1990'

  SELECT * INTO #SECURITY_DATA
    FROM equity_common..security_template
   WHERE 1 = 2

  SELECT * INTO #MARKET_PRICE
    FROM equity_common..market_price_template
   WHERE 1 = 2

  WHILE EXISTS (SELECT 1 FROM #UNIVERSE_MAKEUP_STAGING WHERE universe_dt > @DATE AND security_id IS NULL)
  BEGIN
    SELECT @DATE = MIN(universe_dt)
      FROM #UNIVERSE_MAKEUP_STAGING
     WHERE universe_dt > @DATE
       AND security_id IS NULL

    IF @SOURCE_CD = 'FS'
    BEGIN
      INSERT #SECURITY_DATA (factset_ticker, cusip, sedol, isin, local_ccy_cd, list_exch_cd)
      SELECT u.ticker, u.cusip, u.sedol, u.isin, u.currency_cd, d.decode
        FROM #UNIVERSE_MAKEUP_STAGING u, equity_common..decode d
       WHERE u.universe_dt = @DATE
         AND u.security_id IS NULL
         AND d.item_name = 'EXCHANGE'
         AND u.exchange_nm = d.item_value
    END
    ELSE
    BEGIN
      INSERT #SECURITY_DATA (ticker, cusip, sedol, isin, local_ccy_cd, list_exch_cd)
      SELECT u.ticker, u.cusip, u.sedol, u.isin, u.currency_cd, d.decode
        FROM #UNIVERSE_MAKEUP_STAGING u, equity_common..decode d
       WHERE u.universe_dt = @DATE
         AND u.security_id IS NULL
         AND d.item_name = 'EXCHANGE'
         AND u.exchange_nm = d.item_value
    END

    EXEC equity_common..usp_Security_finalize @data_source_cd='QER', @reference_date=@DATE

    INSERT #MARKET_PRICE (security_id, price_close, price_close_usd)
    SELECT security_id, market_price, market_price_usd
      FROM #SECURITY_DATA

    EXEC equity_common..usp_Market_Price_finalize @data_source_cd='QER', @reference_date=@DATE

    DELETE #SECURITY_DATA
    DELETE #MARKET_PRICE
  END

  DROP TABLE #SECURITY_DATA
  DROP TABLE #MARKET_PRICE
END

EXEC security_id_update @TABLE_NAME='#UNIVERSE_MAKEUP_STAGING', @DATE_COL='universe_dt'

IF @DEBUG = 1
BEGIN
  SELECT '#UNIVERSE_MAKEUP_STAGING (3)'
  SELECT * FROM #UNIVERSE_MAKEUP_STAGING ORDER BY security_id, cusip, sedol, ticker, isin
END

DELETE universe_makeup
  FROM (SELECT DISTINCT s.universe_dt, d.universe_id
          FROM universe_def d, #UNIVERSE_MAKEUP_STAGING s
         WHERE d.universe_cd = s.universe_cd) x
 WHERE universe_makeup.universe_dt = x.universe_dt
   AND universe_makeup.universe_id = x.universe_id

INSERT universe_makeup (universe_dt, universe_id, security_id, weight)
SELECT DISTINCT s.universe_dt, d.universe_id, s.security_id, s.weight
  FROM universe_def d, #UNIVERSE_MAKEUP_STAGING s
 WHERE d.universe_cd = s.universe_cd
   AND s.security_id IS NOT NULL

DROP TABLE #UNIVERSE_MAKEUP_STAGING

RETURN 0
go
IF OBJECT_ID('dbo.universe_makeup_load') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.universe_makeup_load >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.universe_makeup_load >>>'
go
