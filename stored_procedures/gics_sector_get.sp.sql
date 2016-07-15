use QER
go
IF OBJECT_ID('dbo.gics_sector_get') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.gics_sector_get
    IF OBJECT_ID('dbo.gics_sector_get') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.gics_sector_get >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.gics_sector_get >>>'
END
go
CREATE PROCEDURE dbo.gics_sector_get
AS

CREATE TABLE #GICS_SECTOR (
  sector_id		int		NULL,
  gics_sector_num	int		NULL,
  gics_sector_nm	varchar(64)	NULL
)

INSERT #GICS_SECTOR
SELECT DISTINCT NULL, gics_sector_num, upper(gics_sector_nm)
  FROM QER..instrument_characteristics_staging
 WHERE gics_sector_num IS NOT NULL
   AND gics_sector_nm IS NOT NULL

UPDATE #GICS_SECTOR
   SET sector_id = d.sector_id
  FROM QER..sector_model m, QER..sector_def d
 WHERE m.sector_model_cd = 'GICS-S'
   AND m.sector_model_id = d.sector_model_id
   AND d.sector_num = #GICS_SECTOR.gics_sector_num

DELETE #GICS_SECTOR
  FROM QER..sector_def d
 WHERE d.sector_id = #GICS_SECTOR.sector_id
   AND d.sector_nm IS NOT NULL

UPDATE QER..sector_def
   SET sector_nm = r.gics_sector_nm
  FROM #GICS_SECTOR r
 WHERE QER..sector_def.sector_id = r.sector_id
   AND QER..sector_def.sector_nm IS NULL

DELETE #GICS_SECTOR
 WHERE sector_id IS NOT NULL

INSERT QER..sector_def (sector_model_id, sector_num, sector_nm)
SELECT m.sector_model_id, r.gics_sector_num, r.gics_sector_nm
  FROM QER..sector_model m, #GICS_SECTOR r
 WHERE m.sector_model_cd = 'GICS-S'

DROP TABLE #GICS_SECTOR

RETURN 0
go
IF OBJECT_ID('dbo.gics_sector_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.gics_sector_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.gics_sector_get >>>'
go