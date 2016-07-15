use QER
go
IF OBJECT_ID('dbo.instrument_factor_staging_update_cusip2mqa_id') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.instrument_factor_staging_update_cusip2mqa_id
    IF OBJECT_ID('dbo.instrument_factor_staging_update_cusip2mqa_id') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.instrument_factor_staging_update_cusip2mqa_id >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.instrument_factor_staging_update_cusip2mqa_id >>>'
END
go
CREATE PROCEDURE dbo.instrument_factor_staging_update_cusip2mqa_id
AS

UPDATE QER..instrument_factor_staging
   SET mqa_id = c.mqa_id
  FROM QER..cusip2mqa_id c
 WHERE QER..instrument_factor_staging.bdate = c.bdate
   AND QER..instrument_factor_staging.cusip = c.input_cusip
/*
UPDATE QER..instrument_factor_staging
   SET cusip = c.mqa_cusip,
       ticker = c.mqa_ticker,
       gv_key = c.mqa_gv_key
  FROM QER..cusip2mqa_id c
 WHERE QER..instrument_factor_staging.bdate = c.bdate
   AND QER..instrument_factor_staging.mqa_id = c.mqa_id
*/
RETURN 0
go
IF OBJECT_ID('dbo.instrument_factor_staging_update_cusip2mqa_id') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.instrument_factor_staging_update_cusip2mqa_id >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.instrument_factor_staging_update_cusip2mqa_id >>>'
go