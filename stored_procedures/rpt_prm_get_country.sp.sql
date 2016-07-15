use QER
go
IF OBJECT_ID('dbo.rpt_prm_get_country') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rpt_prm_get_country
    IF OBJECT_ID('dbo.rpt_prm_get_country') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rpt_prm_get_country >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rpt_prm_get_country >>>'
END
go
CREATE PROCEDURE dbo.rpt_prm_get_country
AS

SELECT code, decode
  FROM decode
 WHERE item = 'COUNTRY'
   AND decode IS NOT NULL
 ORDER BY decode

RETURN 0
go
IF OBJECT_ID('dbo.rpt_prm_get_country') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_prm_get_country >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_prm_get_country >>>'
go
