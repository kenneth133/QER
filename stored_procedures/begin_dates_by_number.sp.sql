use QER
go
IF OBJECT_ID('dbo.begin_dates_by_number') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.begin_dates_by_number
    IF OBJECT_ID('dbo.begin_dates_by_number') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.begin_dates_by_number >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.begin_dates_by_number >>>'
END
go
CREATE PROCEDURE dbo.begin_dates_by_number @DATE_TYPE varchar(1) = 'D',
                                           @NUMBER_BACK int = 1,
                                           @DATE_FORMAT int = NULL
AS

SELECT @DATE_TYPE = upper(@DATE_TYPE)

IF @DATE_TYPE NOT IN ('D','W','M')
BEGIN
  SELECT 'ERROR: INVALID VALUE PASSED FOR @DATE_TYPE PARAMETER'
  RETURN -1
END

DECLARE @RETURN_CD int

CREATE TABLE #END_DATES_BY_NUM (dt datetime NOT NULL)

EXEC @RETURN_CD = end_dates_by_number @DATE_TYPE, @NUMBER_BACK

CREATE TABLE #DATES (
  dt		datetime	NOT NULL,
  start_dt	datetime	NULL,
  end_dt	datetime	NULL
)

INSERT #DATES
SELECT e.dt, NULL, NULL
  FROM #END_DATES_BY_NUM e

UPDATE #DATES
   SET start_dt = dateadd(dd, 1, dt),
       end_dt   = dateadd(dd, 1, dt)

IF @DATE_TYPE = 'M'
  BEGIN
    UPDATE #DATES
       SET start_dt = dateadd(mm, -1, start_dt)
  END
IF @DATE_TYPE = 'W'
  BEGIN
    UPDATE #DATES
       SET start_dt = dateadd(wk, -1, start_dt)
  END

IF @DATE_FORMAT IS NULL
  BEGIN
    SELECT dt, start_dt, end_dt
      FROM #DATES
  END
ELSE
  BEGIN
    SELECT convert(varchar, dt, @DATE_FORMAT),
           convert(varchar, start_dt, @DATE_FORMAT),
           convert(varchar, end_dt, @DATE_FORMAT)
      FROM #DATES
  END

RETURN 0
go
IF OBJECT_ID('dbo.begin_dates_by_number') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.begin_dates_by_number >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.begin_dates_by_number >>>'
go