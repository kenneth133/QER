use QER
go
IF OBJECT_ID('dbo.end_dates_by_number') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.end_dates_by_number
    IF OBJECT_ID('dbo.end_dates_by_number') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.end_dates_by_number >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.end_dates_by_number >>>'
END
go
CREATE PROCEDURE dbo.end_dates_by_number @DATE_TYPE varchar(1) = 'D',
                                         @NUMBER_BACK int = 1,
                                         @DATE_FORMAT int = NULL
AS

SELECT @DATE_TYPE = upper(@DATE_TYPE)

IF @DATE_TYPE NOT IN ('D','W','M')
BEGIN
  SELECT 'ERROR: INVALID VALUE PASSED FOR @DATE_TYPE PARAMETER'
  RETURN -1
END

DECLARE @COUNTER int,
        @CURRENT datetime,
        @SELECT_BACK bit

SELECT @CURRENT = convert(varchar, getdate(), 112)

IF @DATE_TYPE = 'M'
  BEGIN
    SELECT @CURRENT = dateadd(dd, 1-datepart(dd, @CURRENT), @CURRENT)
  END
ELSE IF @DATE_TYPE = 'W'
  BEGIN
    SELECT @CURRENT = dateadd(dd, -datepart(dw, @CURRENT), @CURRENT)
  END
ELSE IF @DATE_TYPE = 'D'
  BEGIN
    SELECT @CURRENT = dateadd(dd, -1, @CURRENT)
  END

IF NOT EXISTS (SELECT * FROM tempdb.dbo.sysobjects WHERE id = object_id(N'tempdb..[#END_DATES_BY_NUM]') AND type = 'U')
  BEGIN
    SELECT @SELECT_BACK = 1
    CREATE TABLE #END_DATES_BY_NUM (dt datetime NOT NULL)
  END
ELSE
  BEGIN
    SELECT @SELECT_BACK = 0
  END

SELECT @COUNTER = 0

WHILE @COUNTER < @NUMBER_BACK
  BEGIN
    IF @DATE_TYPE = 'M'
      BEGIN
        INSERT #END_DATES_BY_NUM
        SELECT dateadd(mm, -@COUNTER, @CURRENT)
      END
    ELSE IF @DATE_TYPE = 'W'
      BEGIN
        INSERT #END_DATES_BY_NUM
        SELECT dateadd(wk, -@COUNTER, @CURRENT)
      END
    ELSE IF @DATE_TYPE = 'D'
      BEGIN
        INSERT #END_DATES_BY_NUM
        SELECT dateadd(dd, -@COUNTER, @CURRENT)
      END

    SELECT @COUNTER = @COUNTER + 1
  END

IF @DATE_TYPE = 'M'
  BEGIN
    UPDATE #END_DATES_BY_NUM
       SET dt = dateadd(dd, -1, dt)
  END

IF @SELECT_BACK = 1
  BEGIN
    IF @DATE_FORMAT IS NULL
      BEGIN
        SELECT dt
          FROM #END_DATES_BY_NUM
      END
    ELSE
      BEGIN
        SELECT convert(varchar, dt, @DATE_FORMAT)
          FROM #END_DATES_BY_NUM
      END
  END

RETURN 0
go
IF OBJECT_ID('dbo.end_dates_by_number') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.end_dates_by_number >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.end_dates_by_number >>>'
go