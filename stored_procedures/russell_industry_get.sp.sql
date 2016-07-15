use QER
go
IF OBJECT_ID('dbo.russell_industry_get') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.russell_industry_get
    IF OBJECT_ID('dbo.russell_industry_get') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.russell_industry_get >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.russell_industry_get >>>'
END
go
CREATE PROCEDURE dbo.russell_industry_get
AS

CREATE TABLE #RUSSELL_INDUSTRY (
  industry_id		int		NULL,
  russell_industry_num	int		NULL,
  russell_industry_nm	varchar(64)	NULL
)

INSERT #RUSSELL_INDUSTRY
SELECT DISTINCT NULL, russell_industry_num, upper(russell_industry_nm)
  FROM QER..instrument_characteristics_staging
 WHERE russell_industry_num IS NOT NULL
   AND russell_industry_nm IS NOT NULL

UPDATE #RUSSELL_INDUSTRY
   SET industry_id = i.industry_id
  FROM QER..industry_model m, QER..industry i
 WHERE m.industry_model_cd = 'RUSSELL-I'
   AND m.industry_model_id = i.industry_model_id
   AND i.industry_num = #RUSSELL_INDUSTRY.russell_industry_num

DELETE #RUSSELL_INDUSTRY
  FROM QER..industry i
 WHERE i.industry_id = #RUSSELL_INDUSTRY.industry_id
   AND i.industry_nm IS NOT NULL

UPDATE QER..industry
   SET industry_nm = r.russell_industry_nm
  FROM #RUSSELL_INDUSTRY r
 WHERE QER..industry.industry_id = r.industry_id
   AND QER..industry.industry_nm IS NULL

DELETE #RUSSELL_INDUSTRY
 WHERE industry_id IS NOT NULL

INSERT QER..industry (industry_model_id, industry_num, industry_nm)
SELECT m.industry_model_id, r.russell_industry_num, r.russell_industry_nm
  FROM QER..industry_model m, #RUSSELL_INDUSTRY r
 WHERE m.industry_model_cd = 'RUSSELL-I'

DROP TABLE #RUSSELL_INDUSTRY

RETURN 0
go
IF OBJECT_ID('dbo.russell_industry_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.russell_industry_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.russell_industry_get >>>'
go