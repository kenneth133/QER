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
@BDATE datetime
AS

IF @BDATE IS NULL
  BEGIN SELECT 'ERROR: @BDATE IS A REQUIRED PARAMETER' RETURN -1 END

CREATE TABLE #GICS_SEGMENT (
  sector_id			int			NULL,
  gics_sector_num	int			NULL,
  segment_id		int			NULL,
  gics_segment_num	int			NULL,
  gics_segment_nm	varchar(64)	NULL
)

INSERT #GICS_SEGMENT (gics_sector_num, gics_segment_num, gics_segment_nm)
SELECT DISTINCT y.gics_sector_num, y.gics_industry_group_num, UPPER(y.gics_industry_group_name)
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
   AND y.gics_industry_group_name IS NOT NULL

UPDATE #GICS_SEGMENT
   SET sector_id = c.sector_id
  FROM sector_model m, sector_def c
 WHERE m.sector_model_cd = 'GICS-S'
   AND m.sector_model_id = c.sector_model_id
   AND c.sector_num = #GICS_SEGMENT.gics_sector_num

UPDATE #GICS_SEGMENT
   SET segment_id = d.segment_id
  FROM segment_def d
 WHERE d.sector_id = #GICS_SEGMENT.sector_id
   AND d.segment_num = #GICS_SEGMENT.gics_segment_num

DELETE #GICS_SEGMENT
  FROM segment_def d
 WHERE d.segment_id = #GICS_SEGMENT.segment_id
   AND d.segment_nm IS NOT NULL

UPDATE segment_def
   SET segment_nm = g.gics_segment_nm
  FROM #GICS_SEGMENT g
 WHERE segment_def.segment_id = g.segment_id
   AND segment_def.segment_nm IS NULL

DELETE #GICS_SEGMENT
 WHERE segment_id IS NOT NULL

INSERT segment_def (sector_id, segment_num, segment_nm)
SELECT sector_id, gics_segment_num, gics_segment_nm
  FROM #GICS_SEGMENT

UPDATE #GICS_SEGMENT
   SET segment_id = d.segment_id
  FROM segment_def d
 WHERE #GICS_SEGMENT.sector_id = d.sector_id
   AND #GICS_SEGMENT.gics_segment_num = d.segment_num

INSERT sector_makeup
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
