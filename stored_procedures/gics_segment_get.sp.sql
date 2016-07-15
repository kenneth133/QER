use QER
go
IF OBJECT_ID('dbo.gics_segment_get') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.gics_segment_get
    IF OBJECT_ID('dbo.gics_segment_get') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.gics_segment_get >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.gics_segment_get >>>'
END
go
CREATE PROCEDURE dbo.gics_segment_get
AS

CREATE TABLE #GICS_SEGMENT (
  sector_id		int		NULL,
  gics_sector_num	int		NULL,
  segment_id		int		NULL,
  gics_segment_num	int		NULL,
  gics_segment_nm	varchar(64)	NULL
)

INSERT #GICS_SEGMENT (gics_sector_num, gics_segment_num, gics_segment_nm)
SELECT DISTINCT gics_sector_num, gics_segment_num, upper(gics_segment_nm)
  FROM QER..instrument_characteristics_staging
 WHERE gics_segment_num IS NOT NULL
   AND gics_segment_nm IS NOT NULL

UPDATE #GICS_SEGMENT
   SET sector_id = c.sector_id
  FROM QER..sector_model m, QER..sector_def c
 WHERE m.sector_model_cd = 'GICS-S'
   AND m.sector_model_id = c.sector_model_id
   AND c.sector_num = #GICS_SEGMENT.gics_sector_num

UPDATE #GICS_SEGMENT
   SET segment_id = d.segment_id
  FROM QER..segment_def d
 WHERE d.sector_id = #GICS_SEGMENT.sector_id
   AND d.segment_num = #GICS_SEGMENT.gics_segment_num

DELETE #GICS_SEGMENT
  FROM QER..segment_def d
 WHERE d.segment_id = #GICS_SEGMENT.segment_id
   AND d.segment_nm IS NOT NULL

UPDATE QER..segment_def
   SET segment_nm = g.gics_segment_nm
  FROM #GICS_SEGMENT g
 WHERE QER..segment_def.segment_id = g.segment_id
   AND QER..segment_def.segment_nm IS NULL

DELETE #GICS_SEGMENT
 WHERE segment_id IS NOT NULL

INSERT QER..segment_def (sector_id, segment_num, segment_nm)
SELECT sector_id, gics_segment_num, gics_segment_nm
  FROM #GICS_SEGMENT

UPDATE #GICS_SEGMENT
   SET segment_id = d.segment_id
  FROM QER..segment_def d
 WHERE #GICS_SEGMENT.sector_id = d.sector_id
   AND #GICS_SEGMENT.gics_segment_num = d.segment_num

INSERT QER..sector_makeup
SELECT sector_id, 'G', segment_id
  FROM #GICS_SEGMENT

DROP TABLE #GICS_SEGMENT

RETURN 0
go
IF OBJECT_ID('dbo.gics_segment_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.gics_segment_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.gics_segment_get >>>'
go