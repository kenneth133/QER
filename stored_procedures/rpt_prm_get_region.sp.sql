use QER
go
IF OBJECT_ID('dbo.rpt_prm_get_region') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rpt_prm_get_region
    IF OBJECT_ID('dbo.rpt_prm_get_region') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rpt_prm_get_region >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rpt_prm_get_region >>>'
END
go
CREATE PROCEDURE dbo.rpt_prm_get_region @STRATEGY_ID int
AS

SELECT d.region_id, d.region_nm
  FROM strategy g, region_def d
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.region_model_id = d.region_model_id
 ORDER BY d.region_nm

RETURN 0
go
IF OBJECT_ID('dbo.rpt_prm_get_region') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_prm_get_region >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_prm_get_region >>>'
go
