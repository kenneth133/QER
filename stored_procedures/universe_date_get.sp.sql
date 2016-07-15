use QER
go
IF OBJECT_ID('dbo.universe_date_get') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.universe_date_get
    IF OBJECT_ID('dbo.universe_date_get') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.universe_date_get >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.universe_date_get >>>'
END
go
CREATE PROCEDURE dbo.universe_date_get @UNIVERSE_ID int,
                                       @UNIVERSE_DT datetime OUTPUT
AS

DECLARE @HOURS_DIFF int

IF NOT EXISTS (SELECT * FROM QER..universe_makeup WHERE universe_id = @UNIVERSE_ID AND universe_dt = @UNIVERSE_DT)
BEGIN
  SELECT @HOURS_DIFF = min(abs(datediff(hh, @UNIVERSE_DT, universe_dt)))
    FROM QER..universe_makeup
   WHERE universe_id = @UNIVERSE_ID

  SELECT @UNIVERSE_DT = universe_dt
    FROM QER..universe_makeup
   WHERE universe_id = @UNIVERSE_ID
     and abs(datediff(hh, @UNIVERSE_DT, universe_dt)) = @HOURS_DIFF
END

RETURN 0
go
IF OBJECT_ID('dbo.universe_date_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.universe_date_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.universe_date_get >>>'
go