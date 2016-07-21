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
@BDATE datetime
AS

IF @BDATE IS NULL
  BEGIN SELECT 'ERROR: @BDATE IS A REQUIRED PARAMETER' RETURN -1 END

CREATE TABLE #GICS_SECTOR (
  sector_id			int			NULL,
  gics_sector_num	int			NULL,
  gics_sector_nm	varchar(64)	NULL
)

INSERT #GICS_SECTOR
SELECT DISTINCT NULL, y.gics_sector_num, UPPER(y.gics_sector_name)
  FROM equity_common..security y,
      (SELECT security_id FROM universe_makeup WHERE universe_dt = @BDATE
       UNION
       SELECT security_id FROM equity_common..position
        WHERE reference_date = @BDATE
          AND reference_date = effective_date
          AND acct_cd IN (SELECT acct_cd AS [account_cd] FROM equity_common..account
                           WHERE parent IN (SELECT account_cd FROM account)
                          UNION
                          SELECT acct_cd AS [account_cd] FROM equity_common..account
                           WHERE acct_cd IN (SELECT account_cd FROM account)
                          UNION
                          SELECT benchmark_cd AS [account_cd] FROM account)) x
 WHERE y.security_id = x.security_id
   AND y.gics_sector_num IS NOT NULL
   AND y.gics_sector_name IS NOT NULL

UPDATE #GICS_SECTOR
   SET sector_id = d.sector_id
  FROM sector_model m, sector_def d
 WHERE m.sector_model_cd = 'GICS-S'
   AND m.sector_model_id = d.sector_model_id
   AND d.sector_num = #GICS_SECTOR.gics_sector_num

DELETE #GICS_SECTOR
  FROM sector_def d
 WHERE d.sector_id = #GICS_SECTOR.sector_id
   AND d.sector_nm IS NOT NULL

UPDATE sector_def
   SET sector_nm = r.gics_sector_nm
  FROM #GICS_SECTOR r
 WHERE sector_def.sector_id = r.sector_id
   AND sector_def.sector_nm IS NULL

DELETE #GICS_SECTOR
 WHERE sector_id IS NOT NULL

INSERT sector_def (sector_model_id, sector_num, sector_nm)
SELECT m.sector_model_id, r.gics_sector_num, r.gics_sector_nm
  FROM sector_model m, #GICS_SECTOR r
 WHERE m.sector_model_cd = 'GICS-S'

DROP TABLE #GICS_SECTOR

RETURN 0
go
IF OBJECT_ID('dbo.gics_sector_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.gics_sector_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.gics_sector_get >>>'
go
