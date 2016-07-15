use QER
go
IF OBJECT_ID('dbo.business_date_get') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.business_date_get
    IF OBJECT_ID('dbo.business_date_get') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.business_date_get >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.business_date_get >>>'
END
go
CREATE PROCEDURE dbo.business_date_get @DIFF int = 0,
				       @REF_DATE datetime = NULL,
				       @DATE_FORMAT int = NULL,
                                       @RET_DATE datetime = NULL OUTPUT
AS

DECLARE @COUNTER int

IF @REF_DATE IS NULL
  SELECT @REF_DATE = getdate()

SELECT @REF_DATE = convert(varchar, @REF_DATE, 112)

WHILE datepart(dw, @REF_DATE) IN (1,7) OR EXISTS (SELECT * FROM QER..holiday WHERE date = @REF_DATE)
  SELECT @REF_DATE = dateadd(dd, 1, @REF_DATE)

IF @DIFF < 0
  SELECT @COUNTER = -1
ELSE
  SELECT @COUNTER = 1

CREATE TABLE #BIZ_DATE (bdate datetime NOT NULL)

WHILE (SELECT count(*) FROM #BIZ_DATE) != abs(@DIFF)
  BEGIN
    SELECT @RET_DATE = dateadd(dd, @COUNTER, @REF_DATE)

    IF @DIFF < 0
      SELECT @COUNTER = @COUNTER - 1
    ELSE
      SELECT @COUNTER = @COUNTER + 1

    IF datepart(dw, @RET_DATE) NOT IN (1,7) AND NOT EXISTS (SELECT * FROM QER..holiday WHERE date = @RET_DATE)
      BEGIN
        INSERT #BIZ_DATE SELECT @RET_DATE
      END
  END

IF @DIFF > 0
  SELECT @RET_DATE = max(bdate) FROM #BIZ_DATE
ELSE IF @DIFF < 0
  SELECT @RET_DATE = min(bdate) FROM #BIZ_DATE
ELSE 
  SELECT @RET_DATE = @REF_DATE

IF @DATE_FORMAT IS NOT NULL
  BEGIN
    SELECT convert(varchar, @RET_DATE, @DATE_FORMAT)
  END

RETURN 0
go
IF OBJECT_ID('dbo.business_date_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.business_date_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.business_date_get >>>'
go