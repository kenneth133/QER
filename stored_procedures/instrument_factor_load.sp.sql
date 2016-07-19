use QER
go
IF OBJECT_ID('dbo.instrument_factor_load') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.instrument_factor_load
    IF OBJECT_ID('dbo.instrument_factor_load') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.instrument_factor_load >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.instrument_factor_load >>>'
END
go
CREATE PROCEDURE dbo.instrument_factor_load
@SOURCE_CD varchar(8)
AS

SELECT @SOURCE_CD = UPPER(@SOURCE_CD)

IF @SOURCE_CD IS NULL
  BEGIN SELECT 'ERROR: @SOURCE_CD IS A REQUIRED PARAMETER' RETURN -1 END
IF NOT EXISTS (SELECT * FROM decode WHERE item = 'SOURCE_CD' AND code = @SOURCE_CD)
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @SOURCE_CD PARAMETER' RETURN -1 END

DECLARE @NOW datetime
SELECT @NOW = getdate()

DELETE instrument_factor_staging
 WHERE cusip IS NULL
   AND ticker IS NULL
   AND sedol IS NULL
   AND isin IS NULL

CREATE TABLE #INSTRUMENT_FACTOR_STAGING (
  bdate			datetime	NULL,
  security_id	int			NULL,
  ticker		varchar(16)	NULL,
  cusip			varchar(32) NULL,
  sedol			varchar(32) NULL,
  isin			varchar(64)	NULL,
  currency_cd	varchar(3)	NULL,
  exchange_nm	varchar(40)	NULL,
  factor_id		int			NULL,
  factor_value	float		NULL
)

INSERT #INSTRUMENT_FACTOR_STAGING
SELECT i.bdate, NULL, i.ticker, i.cusip, i.sedol, i.isin, i.currency_cd, i.exchange_nm, f.factor_id, i.factor_value
  FROM instrument_factor_staging i, factor f
 WHERE i.factor_cd = f.factor_cd

EXEC security_id_update @TABLE_NAME='#INSTRUMENT_FACTOR_STAGING'

IF EXISTS (SELECT 1 FROM #INSTRUMENT_FACTOR_STAGING WHERE security_id IS NULL)
BEGIN
  DECLARE @DATE datetime
  SELECT @DATE = '1/1/1990'

  SELECT * INTO #SECURITY_DATA
    FROM equity_common..security_template
   WHERE 1 = 2

  SELECT * INTO #MARKET_PRICE
    FROM equity_common..market_price_template
   WHERE 1 = 2

  WHILE EXISTS (SELECT 1 FROM #INSTRUMENT_FACTOR_STAGING WHERE bdate > @DATE AND security_id IS NULL)
  BEGIN
    SELECT @DATE = MIN(bdate)
      FROM #INSTRUMENT_FACTOR_STAGING
     WHERE bdate > @DATE
       AND security_id IS NULL

    IF @SOURCE_CD = 'FS'
    BEGIN
      INSERT #SECURITY_DATA (factset_ticker, cusip, sedol, isin, local_ccy_cd, list_exch_cd)
      SELECT i.ticker, i.cusip, i.sedol, i.isin, i.currency_cd, d.decode
        FROM #INSTRUMENT_FACTOR_STAGING i, equity_common..decode d
       WHERE i.bdate = @DATE
         AND i.security_id IS NULL
         AND d.item_name = 'EXCHANGE'
         AND i.exchange_nm = d.item_value
    END
    ELSE
    BEGIN
      INSERT #SECURITY_DATA (ticker, cusip, sedol, isin, local_ccy_cd, list_exch_cd)
      SELECT i.ticker, i.cusip, i.sedol, i.isin, i.currency_cd, d.decode
        FROM #INSTRUMENT_FACTOR_STAGING i, equity_common..decode d
       WHERE i.bdate = @DATE
         AND i.security_id IS NULL
         AND d.item_name = 'EXCHANGE'
         AND i.exchange_nm = d.item_value
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

EXEC security_id_update @TABLE_NAME='#INSTRUMENT_FACTOR_STAGING'

CREATE NONCLUSTERED INDEX IX_instrument_factor_staging ON #INSTRUMENT_FACTOR_STAGING (bdate, factor_id, security_id)

DELETE #INSTRUMENT_FACTOR_STAGING
  FROM instrument_factor i
 WHERE #INSTRUMENT_FACTOR_STAGING.bdate = i.bdate
   AND #INSTRUMENT_FACTOR_STAGING.factor_id = i.factor_id
   AND #INSTRUMENT_FACTOR_STAGING.security_id = i.security_id
   AND (#INSTRUMENT_FACTOR_STAGING.factor_value = i.factor_value
    OR (#INSTRUMENT_FACTOR_STAGING.factor_value IS NULL AND i.factor_value IS NULL))
   AND i.source_cd = @SOURCE_CD

INSERT instrument_factor
      (bdate, security_id, factor_id, factor_value, update_tm, source_cd)
SELECT bdate, security_id, factor_id, factor_value, @NOW, @SOURCE_CD
  FROM #INSTRUMENT_FACTOR_STAGING
 WHERE security_id IS NOT NULL

DROP TABLE #INSTRUMENT_FACTOR_STAGING

RETURN 0
go
IF OBJECT_ID('dbo.instrument_factor_load') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.instrument_factor_load >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.instrument_factor_load >>>'
go
