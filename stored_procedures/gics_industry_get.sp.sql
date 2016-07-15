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
AS

CREATE TABLE #GICS_INDUSTRY (
  industry_id		int		NULL,
  gics_industry_num	int		NULL,
  gics_industry_nm	varchar(64)	NULL
)

INSERT #GICS_INDUSTRY
SELECT DISTINCT NULL, gics_industry_num, upper(gics_industry_nm)
  FROM QER..instrument_characteristics_staging
 WHERE gics_industry_num IS NOT NULL
   AND gics_industry_nm IS NOT NULL

UPDATE #GICS_INDUSTRY
   SET industry_id = i.industry_id
  FROM QER..industry_model m, QER..industry i
 WHERE m.industry_model_cd = 'GICS-I'
   AND m.industry_model_id = i.industry_model_id
   AND i.industry_num = #GICS_INDUSTRY.gics_industry_num

DELETE #GICS_INDUSTRY
  FROM QER..industry i
 WHERE i.industry_id = #GICS_INDUSTRY.industry_id
   AND i.industry_nm IS NOT NULL

UPDATE QER..industry
   SET industry_nm = g.gics_industry_nm
  FROM #GICS_INDUSTRY g
 WHERE QER..industry.industry_id = g.industry_id
   AND QER..industry.industry_nm IS NULL

DELETE #GICS_INDUSTRY
 WHERE industry_id IS NOT NULL

INSERT QER..industry (industry_model_id, industry_num, industry_nm)
SELECT m.industry_model_id, g.gics_industry_num, g.gics_industry_nm
  FROM QER..industry_model m, #GICS_INDUSTRY g
 WHERE m.industry_model_cd = 'GICS-I'

DROP TABLE #GICS_INDUSTRY

RETURN 0
go
IF OBJECT_ID('dbo.gics_industry_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.gics_industry_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.gics_industry_get >>>'
go