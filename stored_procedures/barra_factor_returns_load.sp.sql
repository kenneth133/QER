use QER
go
IF OBJECT_ID('dbo.barra_factor_returns_load') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.barra_factor_returns_load
    IF OBJECT_ID('dbo.barra_factor_returns_load') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.barra_factor_returns_load >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.barra_factor_returns_load >>>'
END
go
CREATE PROCEDURE dbo.barra_factor_returns_load
AS

UPDATE barra_factor_returns_staging
   SET [DATE] = DATEADD(mm, 1, [DATE])

UPDATE barra_factor_returns_staging
   SET [DATE] = DATEADD(dd, -1, [DATE])

DELETE barra_factor_returns
  FROM barra_factor_returns_staging s
 WHERE barra_factor_returns.month_end_dt = s.[DATE]

INSERT barra_factor_returns
SELECT *
  FROM barra_factor_returns_staging

RETURN 0
go
IF OBJECT_ID('dbo.barra_factor_returns_load') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.barra_factor_returns_load >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.barra_factor_returns_load >>>'
go