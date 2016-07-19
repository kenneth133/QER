use QER
go
IF OBJECT_ID('dbo.gics_sub_industry_get') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.gics_sub_industry_get
    IF OBJECT_ID('dbo.gics_sub_industry_get') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.gics_sub_industry_get >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.gics_sub_industry_get >>>'
END
go
CREATE PROCEDURE dbo.gics_sub_industry_get
@BDATE datetime
AS

IF @BDATE IS NULL
  BEGIN SELECT 'ERROR: @BDATE IS A REQUIRED PARAMETER' RETURN -1 END

CREATE TABLE #GICS_SUB_INDUSTRY (
  industry_id			int			NULL,
  gics_industry_num		int			NULL,
  gics_sub_industry_num	int			NULL,
  gics_sub_industry_nm	varchar(64)	NULL
)

INSERT #GICS_SUB_INDUSTRY (gics_industry_num, gics_sub_industry_num, gics_sub_industry_nm)
SELECT DISTINCT y.gics_industry_num, y.gics_sub_industry_num, UPPER(y.gics_sub_industry_name)
  FROM equity_common..security y,
      (SELECT security_id FROM universe_makeup WHERE universe_dt = @BDATE
       UNION
       SELECT security_id FROM equity_common..position
        WHERE reference_date = @BDATE
          AND reference_date = effective_date
          AND acct_cd IN (SELECT DISTINCT a.acct_cd
                            FROM equity_common..account a,
                                (SELECT account_cd AS [account_cd] FROM account
                                 UNION
                                 SELECT benchmark_cd AS [account_cd] FROM benchmark) q
                           WHERE a.parent = q.account_cd OR a.acct_cd = q.account_cd)) x
 WHERE y.security_id = x.security_id
   AND y.gics_sub_industry_num IS NOT NULL
   AND y.gics_sub_industry_name IS NOT NULL

UPDATE #GICS_SUB_INDUSTRY
   SET industry_id = i.industry_id
  FROM industry_model m, industry i
 WHERE m.industry_model_cd = 'GICS-I'
   AND m.industry_model_id = i.industry_model_id
   AND i.industry_num = #GICS_SUB_INDUSTRY.gics_industry_num

DELETE #GICS_SUB_INDUSTRY
  FROM sub_industry s
 WHERE s.industry_id = #GICS_SUB_INDUSTRY.industry_id
   AND s.sub_industry_num = #GICS_SUB_INDUSTRY.gics_sub_industry_num
   AND s.sub_industry_nm IS NOT NULL

UPDATE sub_industry
   SET sub_industry_nm = g.gics_sub_industry_nm
  FROM #GICS_SUB_INDUSTRY g
 WHERE sub_industry.industry_id = g.industry_id
   AND sub_industry.sub_industry_num = g.gics_sub_industry_num
   AND sub_industry.sub_industry_nm IS NULL

DELETE #GICS_SUB_INDUSTRY
 WHERE gics_sub_industry_num IN (
       SELECT s.sub_industry_num
         FROM industry_model m, industry i, sub_industry s
        WHERE m.industry_model_cd = 'GICS-I'
          AND m.industry_model_id = i.industry_model_id
          AND i.industry_id = s.industry_id)

INSERT sub_industry
SELECT industry_id, gics_sub_industry_num, gics_sub_industry_nm
  FROM #GICS_SUB_INDUSTRY

DROP TABLE #GICS_SUB_INDUSTRY

RETURN 0
go
IF OBJECT_ID('dbo.gics_sub_industry_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.gics_sub_industry_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.gics_sub_industry_get >>>'
go
