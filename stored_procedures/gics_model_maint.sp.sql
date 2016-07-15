use QER
go
IF OBJECT_ID('dbo.gics_model_maint') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.gics_model_maint
    IF OBJECT_ID('dbo.gics_model_maint') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.gics_model_maint >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.gics_model_maint >>>'
END
go
CREATE PROCEDURE dbo.gics_model_maint
AS

EXEC gics_sector_get
EXEC gics_segment_get
EXEC gics_industry_get
EXEC gics_sub_industry_get

CREATE TABLE #GICS_SEGMENT_INDUSTRY (
  segment_id		int	NULL,
  gics_segment_num	int	NULL,
  industry_id		int	NULL,
  gics_industry_num	int	NULL,
)

INSERT #GICS_SEGMENT_INDUSTRY (gics_segment_num, gics_industry_num)
SELECT DISTINCT gics_segment_num, gics_industry_num
  FROM QER..instrument_characteristics_staging
 WHERE gics_segment_num IS NOT NULL
   AND gics_industry_num IS NOT NULL

UPDATE #GICS_SEGMENT_INDUSTRY
   SET segment_id = g.segment_id
  FROM QER..sector_model m, QER..sector_def c, QER..segment_def g
 WHERE m.sector_model_cd = 'GICS-S'
   AND m.sector_model_id = c.sector_model_id
   AND c.sector_id = g.sector_id
   AND g.segment_num = #GICS_SEGMENT_INDUSTRY.gics_segment_num

UPDATE #GICS_SEGMENT_INDUSTRY
   SET industry_id = i.industry_id
  FROM QER..industry_model m, QER..industry i
 WHERE m.industry_model_cd = 'GICS-I'
   AND m.industry_model_id = i.industry_model_id
   AND i.industry_num = #GICS_SEGMENT_INDUSTRY.gics_industry_num

DELETE #GICS_SEGMENT_INDUSTRY
  FROM QER..segment_makeup p
 WHERE #GICS_SEGMENT_INDUSTRY.segment_id = p.segment_id
   AND p.segment_child_type = 'I'
   AND #GICS_SEGMENT_INDUSTRY.industry_id = p.segment_child_id

INSERT QER..segment_makeup
SELECT segment_id, 'I', industry_id
  FROM #GICS_SEGMENT_INDUSTRY

DROP TABLE #GICS_SEGMENT_INDUSTRY

RETURN 0
go
IF OBJECT_ID('dbo.gics_model_maint') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.gics_model_maint >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.gics_model_maint >>>'
go