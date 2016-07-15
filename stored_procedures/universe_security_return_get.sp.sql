use QER
go
IF OBJECT_ID('dbo.universe_security_return_get') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.universe_security_return_get
    IF OBJECT_ID('dbo.universe_security_return_get') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.universe_security_return_get >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.universe_security_return_get >>>'
END
go
CREATE PROCEDURE dbo.universe_security_return_get @BDATE datetime = NULL,
                                                  @UNIVERSE_CD varchar(32),
                                                  @PERIOD_TO_DATE varchar(3),
                                                  @FROM_BDATE datetime = NULL,
                                                  @DEBUG bit = NULL
AS

IF @BDATE IS NULL
BEGIN
  SELECT @BDATE = GETDATE()
  EXEC business_date_get @DIFF=-1, @REF_DATE=@BDATE, @RET_DATE=@BDATE OUTPUT
END
IF @UNIVERSE_CD IS NULL
  BEGIN SELECT 'ERROR: @UNIVERSE_CD IS A REQUIRED PARAMETER' RETURN -1 END
IF NOT EXISTS (SELECT * FROM universe_def WHERE universe_cd = @UNIVERSE_CD)
  BEGIN SELECT 'ERROR: @UNIVERSE_CD="' + @UNIVERSE_CD + '" NOT FOUND' RETURN -1 END
IF @PERIOD_TO_DATE IS NULL AND @FROM_BDATE IS NULL
  BEGIN SELECT 'ERROR: EITHER @PERIOD_TO_DATE OR @FROM_BDATE MUST BE PASSED' RETURN -1 END
IF @PERIOD_TO_DATE IS NOT NULL AND @PERIOD_TO_DATE NOT IN ('YTD', 'QTD', 'MTD', 'WTD')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @PERIOD_TO_DATE PARAMETER' RETURN -1 END
IF @FROM_BDATE IS NOT NULL AND @FROM_BDATE > @BDATE
  BEGIN SELECT 'ERROR: @FROM_BDATE MUST BE LESS THAN @BDATE' RETURN -1 END

DECLARE @MIN_BDATE datetime,
        @MAX_BDATE datetime,
        @MIN_PERIOD_BDATE datetime,
        @PERIOD_NUM int

EXEC business_date_get @DIFF=0, @REF_DATE=@BDATE, @RET_DATE=@MAX_BDATE OUTPUT

IF @PERIOD_TO_DATE = 'YTD'
BEGIN
  SELECT @MIN_PERIOD_BDATE = CONVERT(varchar, DATEPART(YY, @MAX_BDATE)) + '0101'
END
ELSE IF @PERIOD_TO_DATE = 'QTD'
BEGIN
  SELECT @MIN_PERIOD_BDATE = @MAX_BDATE
  SELECT @PERIOD_NUM = DATEPART(QQ, @MAX_BDATE)
  WHILE @PERIOD_NUM = DATEPART(QQ, @MIN_PERIOD_BDATE)
    BEGIN SELECT @MIN_PERIOD_BDATE = DATEADD(DD, -1, @MIN_PERIOD_BDATE) END
  SELECT @MIN_PERIOD_BDATE = DATEADD(DD, 1, @MIN_PERIOD_BDATE)
END
ELSE IF @PERIOD_TO_DATE = 'MTD'
BEGIN
  SELECT @MIN_PERIOD_BDATE = @MAX_BDATE
  SELECT @PERIOD_NUM = DATEPART(MM, @MAX_BDATE)
  WHILE @PERIOD_NUM = DATEPART(MM, @MIN_PERIOD_BDATE)
    BEGIN SELECT @MIN_PERIOD_BDATE = DATEADD(DD, -1, @MIN_PERIOD_BDATE) END
  SELECT @MIN_PERIOD_BDATE = DATEADD(DD, 1, @MIN_PERIOD_BDATE)
END
ELSE IF @PERIOD_TO_DATE = 'WTD'
BEGIN
  SELECT @MIN_PERIOD_BDATE = @MAX_BDATE
  SELECT @PERIOD_NUM = DATEPART(WK, @MAX_BDATE)
  WHILE @PERIOD_NUM = DATEPART(WK, @MIN_PERIOD_BDATE)
    BEGIN SELECT @MIN_PERIOD_BDATE = DATEADD(DD, -1, @MIN_PERIOD_BDATE) END
  SELECT @MIN_PERIOD_BDATE = DATEADD(DD, 1, @MIN_PERIOD_BDATE)
END

IF @FROM_BDATE IS NULL
  BEGIN SELECT @FROM_BDATE = '1/1/2507' END
IF @MIN_PERIOD_BDATE IS NULL
  BEGIN SELECT @MIN_PERIOD_BDATE = '1/1/2507' END

IF @MIN_PERIOD_BDATE < @FROM_BDATE
  BEGIN EXEC business_date_get @DIFF=0, @REF_DATE=@MIN_PERIOD_BDATE, @RET_DATE=@MIN_BDATE OUTPUT END
ELSE
  BEGIN EXEC business_date_get @DIFF=0, @REF_DATE=@FROM_BDATE, @RET_DATE=@MIN_BDATE OUTPUT END

IF @DEBUG=1
BEGIN
  SELECT '@MIN_PERIOD_BDATE', @MIN_PERIOD_BDATE
  SELECT '@FROM_BDATE', @FROM_BDATE
  SELECT '@MIN_BDATE', @MIN_BDATE
  SELECT '@MAX_BDATE', @MAX_BDATE
END

CREATE TABLE #POSITION (
  bdate		datetime	NOT NULL,
  ticker	varchar(16)	NULL,
  cusip		varchar(32)	NOT NULL,
  sedol		varchar(32)	NULL,
  isin		varchar(64)	NULL,
  rtn_p1	float		NULL,
  PRIMARY KEY (bdate, cusip)
)

INSERT #POSITION (bdate, ticker, cusip, sedol, isin)
SELECT p.universe_dt, p.ticker, p.cusip, p.sedol, p.isin
  FROM universe_makeup p, universe_def d
 WHERE d.universe_cd = @UNIVERSE_CD
   AND d.universe_id = p.universe_id
   AND p.universe_dt >= @MIN_BDATE
   AND p.universe_dt <= @MAX_BDATE

IF @DEBUG = 1
BEGIN
  SELECT '#POSITION (1)'
  SELECT * FROM #POSITION ORDER BY bdate, cusip
END

UPDATE #POSITION
   SET rtn_p1 = i.factor_value + 1.0
  FROM instrument_factor i, factor f
 WHERE #POSITION.bdate = i.bdate
   AND #POSITION.cusip = i.cusip
   AND i.factor_id = f.factor_id
   AND f.factor_cd = 'RETURN_1D'

UPDATE #POSITION
   SET rtn_p1 = 1.0
 WHERE rtn_p1 IS NULL

IF @DEBUG = 1
BEGIN
  SELECT '#POSITION (2)'
  SELECT * FROM #POSITION ORDER BY bdate, cusip
END

CREATE TABLE #RETURN (
  ticker	varchar(16)	NULL,
  cusip		varchar(32)	NULL,
  sedol		varchar(32)	NULL,
  isin		varchar(64)	NULL,
  geo_mean_rtn	float		NULL
)

DECLARE @CUSIP varchar(32),
        @ADATE datetime,
        @RTN float

SELECT @CUSIP = ''
WHILE EXISTS (SELECT * FROM #POSITION WHERE cusip > @CUSIP)
BEGIN
  SELECT @CUSIP = MIN(cusip) FROM #POSITION WHERE cusip > @CUSIP
  SELECT @ADATE = MIN(bdate) FROM #POSITION WHERE cusip = @CUSIP
  SELECT @RTN = rtn_p1 FROM #POSITION WHERE cusip = @CUSIP AND bdate = @ADATE

  WHILE EXISTS (SELECT * FROM #POSITION WHERE cusip = @CUSIP AND bdate > @ADATE)
  BEGIN
    SELECT @ADATE = MIN(bdate) FROM #POSITION WHERE cusip = @CUSIP AND bdate > @ADATE
    SELECT @RTN = @RTN * rtn_p1 FROM #POSITION WHERE cusip = @CUSIP AND bdate = @ADATE
  END

  INSERT #RETURN (ticker, cusip, sedol, isin, geo_mean_rtn)
  SELECT ticker, cusip, sedol, isin, @RTN - 1.0
    FROM #POSITION
   WHERE cusip = @CUSIP
     AND bdate = @ADATE
END

DROP TABLE #POSITION

IF @DEBUG = 1
BEGIN
  SELECT '#RETURN (1)'
  SELECT * FROM #RETURN ORDER BY cusip
END

SELECT ticker, cusip, geo_mean_rtn AS [return]
  FROM #RETURN
 ORDER BY cusip

DROP TABLE #RETURN

RETURN 0
go
IF OBJECT_ID('dbo.universe_security_return_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.universe_security_return_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.universe_security_return_get >>>'
go
