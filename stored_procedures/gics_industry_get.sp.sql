use QER
go
IF OBJECT_ID('dbo.gics_industry_get') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.gics_industry_get
    IF OBJECT_ID('dbo.gics_industry_get') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.gics_industry_get >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.gics_industry_get >>>'
END
go
CREATE PROCEDURE dbo.gics_industry_get
@BDATE datetime
AS

IF @BDATE IS NULL
  BEGIN SELECT 'ERROR: @BDATE IS A REQUIRED PARAMETER' RETURN -1 END

CREATE TABLE #GICS_INDUSTRY (
  industry_id		int			NULL,
  gics_industry_num	int			NULL,
  gics_industry_nm	varchar(64)	NULL
)

INSERT #GICS_INDUSTRY
SELECT DISTINCT NULL, y.gics_industry_num, UPPER(y.gics_industry_name)
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
   AND y.gics_industry_num IS NOT NULL
   AND y.gics_industry_name IS NOT NULL

UPDATE #GICS_INDUSTRY
   SET industry_id = i.industry_id
  FROM industry_model m, industry i
 WHERE m.industry_model_cd = 'GICS-I'
   AND m.industry_model_id = i.industry_model_id
   AND i.industry_num = #GICS_INDUSTRY.gics_industry_num

DELETE #GICS_INDUSTRY
  FROM industry i
 WHERE i.industry_id = #GICS_INDUSTRY.industry_id
   AND i.industry_nm IS NOT NULL

UPDATE industry
   SET industry_nm = g.gics_industry_nm
  FROM #GICS_INDUSTRY g
 WHERE industry.industry_id = g.industry_id
   AND industry.industry_nm IS NULL

DELETE #GICS_INDUSTRY
 WHERE industry_id IS NOT NULL

INSERT industry (industry_model_id, industry_num, industry_nm)
SELECT m.industry_model_id, g.gics_industry_num, g.gics_industry_nm
  FROM industry_model m, #GICS_INDUSTRY g
 WHERE m.industry_model_cd = 'GICS-I'

DROP TABLE #GICS_INDUSTRY

RETURN 0
go
IF OBJECT_ID('dbo.gics_industry_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.gics_industry_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.gics_industry_get >>>'
go
