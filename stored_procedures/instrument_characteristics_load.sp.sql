use QER
go
IF OBJECT_ID('dbo.instrument_characteristics_load') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.instrument_characteristics_load
    IF OBJECT_ID('dbo.instrument_characteristics_load') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.instrument_characteristics_load >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.instrument_characteristics_load >>>'
END
go
CREATE PROCEDURE dbo.instrument_characteristics_load
@MODE varchar(16) = 'OVERWRITE',
@IDENTIFIER varchar(16) = 'CUSIP',
@SOURCE_CD varchar(8) = 'MQA'
AS

SELECT @MODE = UPPER(@MODE)
SELECT @IDENTIFIER = UPPER(@IDENTIFIER)
SELECT @SOURCE_CD = UPPER(@SOURCE_CD)

IF @MODE NOT IN ('APPEND', 'OVERWRITE')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @MODE PARAMETER' RETURN -1 END
IF @IDENTIFIER NOT IN ('TICKER', 'CUSIP', 'SEDOL', 'ISIN')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @IDENTIFIER PARAMETER' RETURN -1 END
IF NOT EXISTS (SELECT * FROM decode WHERE item = 'SOURCE_CD' AND code = @SOURCE_CD)
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @SOURCE_CD PARAMETER' RETURN -1 END

DECLARE @NOW datetime
SELECT @NOW = getdate()

DELETE instrument_characteristics_staging
 WHERE cusip IS NULL
   AND ticker IS NULL
   AND sedol IS NULL
   AND isin IS NULL

EXEC currency_decode_update
EXEC exchange_decode_update
EXEC country_decode_update
EXEC sec_type_decode_update
EXEC russell_model_maint
EXEC gics_model_maint

IF @MODE = 'OVERWRITE'
BEGIN
  IF @IDENTIFIER = 'TICKER'
  BEGIN
    DELETE instrument_characteristics
      FROM instrument_characteristics_staging s
     WHERE instrument_characteristics.bdate = s.bdate
       AND instrument_characteristics.ticker = s.ticker
       AND instrument_characteristics.source_cd = @SOURCE_CD
  END
  ELSE IF @IDENTIFIER = 'CUSIP'
  BEGIN
    DELETE instrument_characteristics
      FROM instrument_characteristics_staging s
     WHERE instrument_characteristics.bdate = s.bdate
       AND instrument_characteristics.cusip = s.cusip
       AND instrument_characteristics.source_cd = @SOURCE_CD
  END
  ELSE IF @IDENTIFIER = 'SEDOL'
  BEGIN
    DELETE instrument_characteristics
      FROM instrument_characteristics_staging s
     WHERE instrument_characteristics.bdate = s.bdate
       AND instrument_characteristics.sedol = s.sedol
       AND instrument_characteristics.source_cd = @SOURCE_CD
  END
  ELSE IF @IDENTIFIER = 'ISIN'
  BEGIN
    DELETE instrument_characteristics
      FROM instrument_characteristics_staging s
     WHERE instrument_characteristics.bdate = s.bdate
       AND instrument_characteristics.isin = s.isin
       AND instrument_characteristics.source_cd = @SOURCE_CD
  END
END
ELSE IF @MODE = 'APPEND'
BEGIN
  IF @IDENTIFIER = 'TICKER'
  BEGIN
    DELETE instrument_characteristics_staging
      FROM instrument_characteristics i
     WHERE instrument_characteristics_staging.bdate = i.bdate
       AND instrument_characteristics_staging.ticker = i.ticker
       AND i.source_cd = @SOURCE_CD
  END
  ELSE IF @IDENTIFIER = 'CUSIP'
  BEGIN
    DELETE instrument_characteristics_staging
      FROM instrument_characteristics i
     WHERE instrument_characteristics_staging.bdate = i.bdate
       AND instrument_characteristics_staging.cusip = i.cusip
       AND i.source_cd = @SOURCE_CD
  END
  ELSE IF @IDENTIFIER = 'SEDOL'
  BEGIN
    DELETE instrument_characteristics_staging
      FROM instrument_characteristics i
     WHERE instrument_characteristics_staging.bdate = i.bdate
       AND instrument_characteristics_staging.sedol = i.sedol
       AND i.source_cd = @SOURCE_CD
  END
  ELSE IF @IDENTIFIER = 'ISIN'
  BEGIN
    DELETE instrument_characteristics_staging
      FROM instrument_characteristics i
     WHERE instrument_characteristics_staging.bdate = i.bdate
       AND instrument_characteristics_staging.isin = i.isin
       AND i.source_cd = @SOURCE_CD
  END
END

INSERT instrument_characteristics
      (bdate, mqa_id, ticker, cusip, sedol, isin, gv_key, imnt_nm,
       price_to_book, price_close, price_close_local,
       currency_local, exchange, country, sec_type,
       mkt_cap, volume, volatility, beta, quality,
       gics_sector_num, gics_segment_num, gics_industry_num, gics_sub_industry_num,
       russell_sector_num, russell_industry_num,
       update_tm, source_cd)
SELECT bdate, mqa_id, ticker, cusip, sedol, isin, gv_key, imnt_nm,
       price_to_book, price_close, price_close_local,
       currency_local, exchange, country, sectype,
       mktcap, volume, volatility, beta, quality,
       gics_sector_num, gics_segment_num, gics_industry_num, gics_sub_industry_num,
       russell_sector_num, russell_industry_num,
       @NOW, @SOURCE_CD
  FROM instrument_characteristics_staging

RETURN 0
go
IF OBJECT_ID('dbo.instrument_characteristics_load') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.instrument_characteristics_load >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.instrument_characteristics_load >>>'
go