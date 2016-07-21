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
@BDATE datetime
AS

IF @BDATE IS NULL
  BEGIN SELECT 'ERROR: @BDATE IS A REQUIRED PARAMETER' RETURN -1 END

EXEC gics_sector_get @BDATE
EXEC gics_segment_get @BDATE
EXEC gics_industry_get @BDATE
EXEC gics_sub_industry_get @BDATE

CREATE TABLE #GICS_SEGMENT_INDUSTRY (
  segment_id		int	NULL,
  gics_segment_num	int	NULL,
  industry_id		int	NULL,
  gics_industry_num	int	NULL,
)

INSERT #GICS_SEGMENT_INDUSTRY (gics_segment_num, gics_industry_num)
SELECT DISTINCT y.gics_industry_group_num, y.gics_industry_num
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
   AND y.gics_industry_group_num IS NOT NULL
   AND y.gics_industry_num IS NOT NULL

UPDATE #GICS_SEGMENT_INDUSTRY
   SET segment_id = g.segment_id
  FROM sector_model m, sector_def c, segment_def g
 WHERE m.sector_model_cd = 'GICS-S'
   AND m.sector_model_id = c.sector_model_id
   AND c.sector_id = g.sector_id
   AND g.segment_num = #GICS_SEGMENT_INDUSTRY.gics_segment_num

UPDATE #GICS_SEGMENT_INDUSTRY
   SET industry_id = i.industry_id
  FROM industry_model m, industry i
 WHERE m.industry_model_cd = 'GICS-I'
   AND m.industry_model_id = i.industry_model_id
   AND i.industry_num = #GICS_SEGMENT_INDUSTRY.gics_industry_num

DELETE #GICS_SEGMENT_INDUSTRY
  FROM segment_makeup p
 WHERE #GICS_SEGMENT_INDUSTRY.segment_id = p.segment_id
   AND p.segment_child_type = 'I'
   AND #GICS_SEGMENT_INDUSTRY.industry_id = p.segment_child_id

INSERT segment_makeup
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
