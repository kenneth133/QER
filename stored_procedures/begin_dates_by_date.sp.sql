use QER
go
IF OBJECT_ID('dbo.begin_dates_by_date') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.begin_dates_by_date
    IF OBJECT_ID('dbo.begin_dates_by_date') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.begin_dates_by_date >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.begin_dates_by_date >>>'
END
go
CREATE PROCEDURE dbo.begin_dates_by_date @DATE_TYPE varchar(2) = 'D',
                                         @DATE1 datetime = NULL,
                                         @DATE2 datetime = NULL,
                                         @DATE_FORMAT int = NULL
AS

SELECT @DATE_TYPE = upper(@DATE_TYPE)

IF @DATE_TYPE NOT IN ('YY','YYYY','QQ','Q','MM','M','WK','WW','DD','D')
BEGIN
  SELECT 'ERROR: INVALID VALUE PASSED FOR @DATE_TYPE PARAMETER'
  RETURN -1
END

DECLARE @COUNTER int,
        @DIFF int,
        @MAX_DATE datetime,
        @SELECT_BACK bit

IF @DATE1 IS NULL
  BEGIN
    SELECT @DATE1 = convert(varchar, getdate(), 112)
  END
IF @DATE2 IS NULL
  BEGIN
    SELECT @DATE2 = convert(varchar, getdate(), 112)
  END

IF @DATE1 > @DATE2
  BEGIN
    SELECT @MAX_DATE = @DATE1
  END
ELSE
  BEGIN
    SELECT @MAX_DATE = @DATE2
  END

SELECT @MAX_DATE = convert(varchar, @MAX_DATE, 112)

IF @DATE_TYPE IN ('YY','YYYY')
  BEGIN
    SELECT @DIFF = 1 + abs(datediff(yy, @DATE1, @DATE2))
    SELECT @MAX_DATE = '1/1/' + convert(varchar, datepart(yy, @MAX_DATE))
  END
ELSE IF @DATE_TYPE IN ('QQ','Q')
  BEGIN
    SELECT @DIFF = 1 + abs(datediff(qq, @DATE1, @DATE2))
    SELECT @COUNTER = datepart(qq, @MAX_DATE)
    WHILE @COUNTER = datepart(qq, @MAX_DATE)
      BEGIN
        SELECT @MAX_DATE = dateadd(dd, -1, @MAX_DATE)
      END
    SELECT @MAX_DATE = dateadd(dd, 1, @MAX_DATE)
  END
ELSE IF @DATE_TYPE IN ('MM','M')
  BEGIN
    SELECT @DIFF = 1 + abs(datediff(mm, @DATE1, @DATE2))
    SELECT @MAX_DATE = dateadd(dd, 1-datepart(dd, @MAX_DATE), @MAX_DATE)
  END
ELSE IF @DATE_TYPE IN ('WK','WW')
  BEGIN
    SELECT @DIFF = 1 + abs(datediff(wk, @DATE1, @DATE2))
    SELECT @MAX_DATE = dateadd(dd, 1-datepart(dw, @MAX_DATE), @MAX_DATE)
  END
ELSE IF @DATE_TYPE IN ('DD','D')
  BEGIN
    SELECT @DIFF = 1 + abs(datediff(dd, @DATE1, @DATE2))
  END

IF NOT EXISTS (SELECT * FROM tempdb.dbo.sysobjects WHERE id = object_id(N'tempdb..[#END_DATES_BY_DATE]') AND type = 'U')
  BEGIN
    SELECT @SELECT_BACK = 1
    CREATE TABLE #END_DATES_BY_DATE (dt datetime NOT NULL)
  END
ELSE
  BEGIN
    SELECT @SELECT_BACK = 0
  END

SELECT @COUNTER = 0

WHILE @COUNTER < @DIFF
  BEGIN
    IF @DATE_TYPE IN ('YY','YYYY')
      BEGIN
        INSERT #END_DATES_BY_DATE
        SELECT dateadd(yy, -@COUNTER, @MAX_DATE)
      END
    ELSE IF @DATE_TYPE IN ('QQ','Q')
      BEGIN
        INSERT #END_DATES_BY_DATE
        SELECT dateadd(qq, -@COUNTER, @MAX_DATE)
      END
    ELSE IF @DATE_TYPE IN ('MM','M')
      BEGIN
        INSERT #END_DATES_BY_DATE
        SELECT dateadd(mm, -@COUNTER, @MAX_DATE)
      END
    ELSE IF @DATE_TYPE IN ('WK','WW')
      BEGIN
        INSERT #END_DATES_BY_DATE
        SELECT dateadd(wk, -@COUNTER, @MAX_DATE)
      END
    ELSE IF @DATE_TYPE IN ('DD','D')
      BEGIN
        INSERT #END_DATES_BY_DATE
        SELECT dateadd(dd, -@COUNTER, @MAX_DATE)
      END

    SELECT @COUNTER = @COUNTER + 1
  END

IF @SELECT_BACK = 1
  BEGIN
    IF @DATE_FORMAT IS NULL
      BEGIN
        SELECT dt
          FROM #END_DATES_BY_DATE
      END
    ELSE
      BEGIN
        SELECT convert(varchar, dt, @DATE_FORMAT)
          FROM #END_DATES_BY_DATE
      END
  END

RETURN 0
go
IF OBJECT_ID('dbo.begin_dates_by_date') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.begin_dates_by_date >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.begin_dates_by_date >>>'
go