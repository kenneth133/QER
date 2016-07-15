use QER
go
IF OBJECT_ID('dbo.russell_model_maint') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.russell_model_maint
    IF OBJECT_ID('dbo.russell_model_maint') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.russell_model_maint >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.russell_model_maint >>>'
END
go
CREATE PROCEDURE dbo.russell_model_maint
AS

EXEC russell_sector_get
EXEC russell_industry_get

CREATE TABLE #RUSSELL_SECTOR_INDUSTRY (
  sector_id		int	NULL,
  russell_sector_num	int	NULL,
  industry_id		int	NULL,
  russell_industry_num	int	NULL
)

INSERT #RUSSELL_SECTOR_INDUSTRY
SELECT DISTINCT NULL, russell_sector_num, NULL, russell_industry_num
  FROM QER..instrument_characteristics_staging
 WHERE russell_sector_num IS NOT NULL
   AND russell_industry_num IS NOT NULL

UPDATE #RUSSELL_SECTOR_INDUSTRY
   SET sector_id = d.sector_id
  FROM QER..sector_model m, QER..sector_def d
 WHERE m.sector_model_cd = 'RUSSELL-S'
   AND m.sector_model_id = d.sector_model_id
   AND d.sector_num = #RUSSELL_SECTOR_INDUSTRY.russell_sector_num

UPDATE #RUSSELL_SECTOR_INDUSTRY
   SET industry_id = i.industry_id
  FROM QER..industry_model m, QER..industry i
 WHERE m.industry_model_cd = 'RUSSELL-I'
   AND m.industry_model_id = i.industry_model_id
   AND i.industry_num = #RUSSELL_SECTOR_INDUSTRY.russell_industry_num

DELETE #RUSSELL_SECTOR_INDUSTRY
  FROM QER..sector_makeup p
 WHERE p.sector_id = #RUSSELL_SECTOR_INDUSTRY.sector_id
   AND p.sector_child_type = 'I'
   AND p.sector_child_id = #RUSSELL_SECTOR_INDUSTRY.industry_id

INSERT QER..sector_makeup
SELECT sector_id, 'I', industry_id
  FROM #RUSSELL_SECTOR_INDUSTRY

DROP TABLE #RUSSELL_SECTOR_INDUSTRY

RETURN 0
go
IF OBJECT_ID('dbo.russell_model_maint') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.russell_model_maint >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.russell_model_maint >>>'
go