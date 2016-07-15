use QER
go
IF OBJECT_ID('dbo.rpt_prm_get_sector') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rpt_prm_get_sector
    IF OBJECT_ID('dbo.rpt_prm_get_sector') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rpt_prm_get_sector >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rpt_prm_get_sector >>>'
END
go
CREATE PROCEDURE dbo.rpt_prm_get_sector @STRATEGY_ID int
AS

SELECT c.sector_id, c.sector_nm
  FROM strategy s, factor_model m, sector_def c
 WHERE s.strategy_id = @STRATEGY_ID
   AND s.factor_model_id = m.factor_model_id
   AND m.sector_model_id = c.sector_model_id
   AND c.sector_nm != 'UNASSIGNED'
 ORDER BY c.sector_nm

RETURN 0
go
IF OBJECT_ID('dbo.rpt_prm_get_sector') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_prm_get_sector >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_prm_get_sector >>>'
go
