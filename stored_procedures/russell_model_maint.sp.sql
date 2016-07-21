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
@BDATE datetime
AS

IF @BDATE IS NULL
  BEGIN SELECT 'ERROR: @BDATE IS A REQUIRED PARAMETER' RETURN -1 END

EXEC russell_sector_get @BDATE
EXEC russell_industry_get @BDATE

CREATE TABLE #RUSSELL_SECTOR_INDUSTRY (
  sector_id				int	NULL,
  russell_sector_num	int	NULL,
  industry_id			int	NULL,
  russell_industry_num	int	NULL
)

INSERT #RUSSELL_SECTOR_INDUSTRY
SELECT DISTINCT NULL, y.russell_sector_num, NULL, y.russell_industry_num
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
   AND y.russell_sector_num IS NOT NULL
   AND y.russell_industry_num IS NOT NULL

UPDATE #RUSSELL_SECTOR_INDUSTRY
   SET sector_id = d.sector_id
  FROM sector_model m, sector_def d
 WHERE m.sector_model_cd = 'RUSSELL-S'
   AND m.sector_model_id = d.sector_model_id
   AND d.sector_num = #RUSSELL_SECTOR_INDUSTRY.russell_sector_num

UPDATE #RUSSELL_SECTOR_INDUSTRY
   SET industry_id = i.industry_id
  FROM industry_model m, industry i
 WHERE m.industry_model_cd = 'RUSSELL'
   AND m.industry_model_id = i.industry_model_id
   AND i.industry_num = #RUSSELL_SECTOR_INDUSTRY.russell_industry_num

DELETE #RUSSELL_SECTOR_INDUSTRY
  FROM sector_makeup p
 WHERE p.sector_id = #RUSSELL_SECTOR_INDUSTRY.sector_id
   AND p.sector_child_type = 'I'
   AND p.sector_child_id = #RUSSELL_SECTOR_INDUSTRY.industry_id

INSERT sector_makeup
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
