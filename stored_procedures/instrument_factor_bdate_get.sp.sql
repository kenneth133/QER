use QER
go
IF OBJECT_ID('dbo.instrument_factor_bdate_get') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.instrument_factor_bdate_get
    IF OBJECT_ID('dbo.instrument_factor_bdate_get') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.instrument_factor_bdate_get >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.instrument_factor_bdate_get >>>'
END
go
CREATE PROCEDURE dbo.instrument_factor_bdate_get @FACTOR_ID int,
                                                 @FACTOR_SOURCE_CD varchar(8),
                                                 @UNIVERSE_ID int,
                                                 @UNIVERSE_DT datetime,
                                                 @BDATE datetime OUTPUT
AS

EXEC QER..universe_date_get @UNIVERSE_ID, @UNIVERSE_DT OUTPUT

IF @FACTOR_SOURCE_CD IS NULL
BEGIN
  WHILE NOT EXISTS (SELECT * FROM QER..instrument_factor 
                     WHERE bdate = @BDATE
                       AND factor_id = @FACTOR_ID
                       AND cusip IN (SELECT cusip FROM QER..universe_makeup
                                      WHERE universe_id = @UNIVERSE_ID
                                        AND universe_dt = @UNIVERSE_DT))
  BEGIN
    SELECT @BDATE = dateadd(dd, -1, @BDATE)
  END
END
ELSE--@FACTOR_SOURCE_CD IS NOT NULL
BEGIN
  WHILE NOT EXISTS (SELECT * FROM QER..instrument_factor 
                     WHERE bdate = @BDATE
                       AND factor_id = @FACTOR_ID
                       AND source_cd = @FACTOR_SOURCE_CD
                       AND cusip IN (SELECT cusip FROM QER..universe_makeup
                                      WHERE universe_id = @UNIVERSE_ID
                                        AND universe_dt = @UNIVERSE_DT))
  BEGIN
    SELECT @BDATE = dateadd(dd, -1, @BDATE)
  END
END

RETURN 0
go
IF OBJECT_ID('dbo.instrument_factor_bdate_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.instrument_factor_bdate_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.instrument_factor_bdate_get >>>'
go