use QER
go
IF OBJECT_ID('dbo.russell_sector_get') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.russell_sector_get
    IF OBJECT_ID('dbo.russell_sector_get') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.russell_sector_get >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.russell_sector_get >>>'
END
go
CREATE PROCEDURE dbo.russell_sector_get
AS

CREATE TABLE #RUSSELL_SECTOR (
  sector_id		int		NULL,
  russell_sector_num	int		NULL,
  russell_sector_nm	varchar(64)	NULL
)

INSERT #RUSSELL_SECTOR
SELECT DISTINCT NULL, russell_sector_num, upper(russell_sector_nm)
  FROM QER..instrument_characteristics_staging
 WHERE russell_sector_num IS NOT NULL
   AND russell_sector_nm IS NOT NULL

UPDATE #RUSSELL_SECTOR
   SET sector_id = d.sector_id
  FROM QER..sector_model m, QER..sector_def d
 WHERE m.sector_model_cd = 'RUSSELL-S'
   AND m.sector_model_id = d.sector_model_id
   AND d.sector_num = #RUSSELL_SECTOR.russell_sector_num

DELETE #RUSSELL_SECTOR
  FROM QER..sector_def d
 WHERE d.sector_id = #RUSSELL_SECTOR.sector_id
   AND d.sector_nm IS NOT NULL

UPDATE QER..sector_def
   SET sector_nm = r.russell_sector_nm
  FROM #RUSSELL_SECTOR r
 WHERE QER..sector_def.sector_id = r.sector_id
   AND QER..sector_def.sector_nm IS NULL

DELETE #RUSSELL_SECTOR
 WHERE sector_id IS NOT NULL

INSERT QER..sector_def (sector_model_id, sector_num, sector_nm)
SELECT m.sector_model_id, r.russell_sector_num, r.russell_sector_nm
  FROM QER..sector_model m, #RUSSELL_SECTOR r
 WHERE m.sector_model_cd = 'RUSSELL-S'

DROP TABLE #RUSSELL_SECTOR

RETURN 0
go
IF OBJECT_ID('dbo.russell_sector_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.russell_sector_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.russell_sector_get >>>'
go