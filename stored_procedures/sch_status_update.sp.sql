use QER
go
IF OBJECT_ID('dbo.sch_status_update') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.sch_status_update
    IF OBJECT_ID('dbo.sch_status_update') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.sch_status_update >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.sch_status_update >>>'
END
go
CREATE PROCEDURE dbo.sch_status_update @JOB_NAME varchar(64) = NULL,
                                       @STATUS   varchar(1)  = NULL
AS

IF @JOB_NAME IS NULL
  BEGIN
    SELECT 'ERROR: @JOB_NAME PARAMETER MUST BE PASSED'
    RETURN -1
  END

IF @STATUS NOT IN ('E','C','F')
  BEGIN
    SELECT 'ERROR: @STATUS PARAMETER MUST BE PASSED'
    RETURN -1
  END

DECLARE @PREV_BUS_DAY datetime
EXEC business_date_get -1, NULL, NULL, @PREV_BUS_DAY OUTPUT

IF @STATUS = 'E'
  BEGIN
    INSERT QER..sch_status
    VALUES (@JOB_NAME, @PREV_BUS_DAY, getdate(), NULL, 'EXECUTING')
  END
ELSE IF @STATUS = 'C'
  BEGIN
    UPDATE QER..sch_status
       SET ended = getdate(),
           status = 'COMPLETED'
     WHERE job_name = @JOB_NAME
       AND prev_bus_day = @PREV_BUS_DAY
       AND status = 'EXECUTING'
  END
ELSE IF @STATUS = 'F'
  BEGIN
    UPDATE QER..sch_status
       SET ended = getdate(),
           status = 'FAILED'
     WHERE job_name = @JOB_NAME
       AND prev_bus_day = @PREV_BUS_DAY
       AND status = 'EXECUTING'
  END

RETURN 0
go
IF OBJECT_ID('dbo.sch_status_update') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.sch_status_update >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.sch_status_update >>>'
go