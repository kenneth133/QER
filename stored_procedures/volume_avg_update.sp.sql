use QER
go
IF OBJECT_ID('dbo.volume_avg_update') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.volume_avg_update
    IF OBJECT_ID('dbo.volume_avg_update') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.volume_avg_update >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.volume_avg_update >>>'
END
go
CREATE PROCEDURE dbo.volume_avg_update @DATE1 datetime,
                                       @DATE2 datetime = NULL,
                                       @NUM_DAYS int
AS

/* NOTE: THIS PROCEDURE CONVERTS "DAILY TRADING VOLUME IN SHARES" TO "AVERAGE TRADING VOLUME IN MARKET-VALUE" */

IF @DATE1 IS NULL
  BEGIN SELECT 'ERROR: @DATE1 IS A REQUIRED PARAMETER' RETURN -1 END
IF @NUM_DAYS IS NULL
  BEGIN SELECT 'ERROR: @NUM_DAYS IS A REQUIRED PARAMETER' RETURN -1 END
IF @NUM_DAYS NOT IN (30, 60, 90)
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @NUM_DAYS PARAMETER' RETURN -1 END
IF @NUM_DAYS > 0
  BEGIN SELECT @NUM_DAYS = -1 * @NUM_DAYS END

CREATE TABLE #VOL (
  bdate		datetime	NOT NULL,
  cusip		varchar(32)	NULL,
  vol_avg	float		NULL
)

IF @DATE2 IS NOT NULL
BEGIN
  INSERT #VOL (bdate, cusip, vol_avg)
  SELECT i1.bdate, i1.cusip, AVG(i2.volume * i2.price_close)
    FROM instrument_characteristics i1,
         instrument_characteristics i2
   WHERE i2.bdate >= DATEADD(DD, @NUM_DAYS, i1.bdate)
     AND i2.bdate < i1.bdate
     AND i1.cusip = i2.cusip
     AND i1.bdate >= @DATE1
     AND i1.bdate <= @DATE2
   GROUP BY i1.bdate, i1.cusip
END
ELSE
BEGIN
  INSERT #VOL (bdate, cusip, vol_avg)
  SELECT i1.bdate, i1.cusip, AVG(i2.volume * i2.price_close)
    FROM instrument_characteristics i1,
         instrument_characteristics i2
   WHERE i2.bdate >= DATEADD(DD, @NUM_DAYS, i1.bdate)
     AND i2.bdate < i1.bdate
     AND i1.cusip = i2.cusip
     AND i1.bdate = @DATE1
   GROUP BY i1.bdate, i1.cusip
END

IF ABS(@NUM_DAYS) = 30
BEGIN
  UPDATE instrument_characteristics
     SET volume30 = v.vol_avg
    FROM #VOL v
   WHERE instrument_characteristics.bdate = v.bdate
     AND instrument_characteristics.cusip = v.cusip
END
ELSE IF ABS(@NUM_DAYS) = 60
BEGIN
  UPDATE instrument_characteristics
     SET volume60 = v.vol_avg
    FROM #VOL v
   WHERE instrument_characteristics.bdate = v.bdate
     AND instrument_characteristics.cusip = v.cusip
END
ELSE IF ABS(@NUM_DAYS) = 90
BEGIN
  UPDATE instrument_characteristics
     SET volume90 = v.vol_avg
    FROM #VOL v
   WHERE instrument_characteristics.bdate = v.bdate
     AND instrument_characteristics.cusip = v.cusip
END

DROP TABLE #VOL

RETURN 0
go
IF OBJECT_ID('dbo.volume_avg_update') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.volume_avg_update >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.volume_avg_update >>>'
go