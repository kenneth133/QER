use QER
go
IF OBJECT_ID('dbo.rpt_prm_get_bdate') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rpt_prm_get_bdate
    IF OBJECT_ID('dbo.rpt_prm_get_bdate') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rpt_prm_get_bdate >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rpt_prm_get_bdate >>>'
END
go
CREATE PROCEDURE dbo.rpt_prm_get_bdate
AS

SELECT DISTINCT a.bdate  
  FROM scores a, strategy b  
 WHERE a.strategy_id = b.strategy_id
   and a.bdate >= '20070101'  
   and dbo.udf_check_user_access(b.strategy_id) = 'Y'
 ORDER BY a.bdate DESC

RETURN 0
go
IF OBJECT_ID('dbo.rpt_prm_get_bdate') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_prm_get_bdate >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_prm_get_bdate >>>'
go
