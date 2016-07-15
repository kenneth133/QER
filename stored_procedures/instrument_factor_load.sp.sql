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
@IDENTIFIER varchar(16) = 'CUSIP',
@SOURCE_CD varchar(8)
AS

SELECT @IDENTIFIER = UPPER(@IDENTIFIER)
SELECT @SOURCE_CD = UPPER(@SOURCE_CD)

IF @IDENTIFIER NOT IN ('TICKER', 'CUSIP', 'SEDOL', 'ISIN')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @IDENTIFIER PARAMETER' RETURN -1 END
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
  bdate		datetime	NULL,
  mqa_id	varchar(32)	NULL,
  ticker	varchar(16)	NULL,
  cusip		varchar(32) NULL,
  sedol		varchar(32) NULL,
  isin		varchar(64)	NULL,
  gv_key	int			NULL,
  factor_id	int			NULL,
  factor_value	float	NULL
)

INSERT #INSTRUMENT_FACTOR_STAGING
SELECT i.bdate, i.mqa_id, i.ticker, i.cusip, i.sedol, i.isin, i.gv_key, f.factor_id, i.factor_value
  FROM instrument_factor_staging i, factor f
 WHERE i.factor_cd = f.factor_cd

CREATE NONCLUSTERED INDEX IX_instrument_factor_staging ON #INSTRUMENT_FACTOR_STAGING (bdate, factor_id, cusip)

IF @IDENTIFIER = 'TICKER'
BEGIN
  DELETE #INSTRUMENT_FACTOR_STAGING
    FROM instrument_factor i
   WHERE #INSTRUMENT_FACTOR_STAGING.bdate = i.bdate
     AND #INSTRUMENT_FACTOR_STAGING.factor_id = i.factor_id
     AND #INSTRUMENT_FACTOR_STAGING.ticker = i.ticker
     AND (#INSTRUMENT_FACTOR_STAGING.factor_value = i.factor_value
      OR (#INSTRUMENT_FACTOR_STAGING.factor_value IS NULL AND i.factor_value IS NULL))
     AND i.source_cd = @SOURCE_CD
END
ELSE IF @IDENTIFIER = 'CUSIP'
BEGIN
  DELETE #INSTRUMENT_FACTOR_STAGING
    FROM instrument_factor i WITH (INDEX(IX_instrument_factor_1))
   WHERE #INSTRUMENT_FACTOR_STAGING.bdate = i.bdate
     AND #INSTRUMENT_FACTOR_STAGING.factor_id = i.factor_id
     AND #INSTRUMENT_FACTOR_STAGING.cusip = i.cusip
     AND (#INSTRUMENT_FACTOR_STAGING.factor_value = i.factor_value
      OR (#INSTRUMENT_FACTOR_STAGING.factor_value IS NULL AND i.factor_value IS NULL))
     AND i.source_cd = @SOURCE_CD
END
ELSE IF @IDENTIFIER = 'SEDOL'
BEGIN
  DELETE #INSTRUMENT_FACTOR_STAGING
    FROM instrument_factor i
   WHERE #INSTRUMENT_FACTOR_STAGING.bdate = i.bdate
     AND #INSTRUMENT_FACTOR_STAGING.factor_id = i.factor_id
     AND #INSTRUMENT_FACTOR_STAGING.sedol = i.sedol
     AND (#INSTRUMENT_FACTOR_STAGING.factor_value = i.factor_value
      OR (#INSTRUMENT_FACTOR_STAGING.factor_value IS NULL AND i.factor_value IS NULL))
     AND i.source_cd = @SOURCE_CD
END
ELSE IF @IDENTIFIER = 'ISIN'
BEGIN
  DELETE #INSTRUMENT_FACTOR_STAGING
    FROM instrument_factor i
   WHERE #INSTRUMENT_FACTOR_STAGING.bdate = i.bdate
     AND #INSTRUMENT_FACTOR_STAGING.factor_id = i.factor_id
     AND #INSTRUMENT_FACTOR_STAGING.isin = i.isin
     AND (#INSTRUMENT_FACTOR_STAGING.factor_value = i.factor_value
      OR (#INSTRUMENT_FACTOR_STAGING.factor_value IS NULL AND i.factor_value IS NULL))
     AND i.source_cd = @SOURCE_CD
END

INSERT instrument_factor
      (bdate, mqa_id, ticker, cusip, sedol, isin, gv_key, factor_id, factor_value, update_tm, source_cd)
SELECT bdate, mqa_id, ticker, cusip, sedol, isin, gv_key, factor_id, factor_value, @NOW, @SOURCE_CD
  FROM #INSTRUMENT_FACTOR_STAGING

DROP TABLE #INSTRUMENT_FACTOR_STAGING

RETURN 0
go
IF OBJECT_ID('dbo.instrument_factor_load') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.instrument_factor_load >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.instrument_factor_load >>>'
go