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
AS

CREATE TABLE #GICS_SUB_INDUSTRY (
  industry_id		int		NULL,
  gics_industry_num	int		NULL,
  gics_sub_industry_num	int		NULL,
  gics_sub_industry_nm	varchar(64)	NULL
)

INSERT #GICS_SUB_INDUSTRY (gics_industry_num, gics_sub_industry_num, gics_sub_industry_nm)
SELECT DISTINCT gics_industry_num, gics_sub_industry_num, upper(gics_sub_industry_nm)
  FROM QER..instrument_characteristics_staging
 WHERE gics_sub_industry_num IS NOT NULL
   AND gics_sub_industry_nm IS NOT NULL

UPDATE #GICS_SUB_INDUSTRY
   SET industry_id = i.industry_id
  FROM QER..industry_model m, QER..industry i
 WHERE m.industry_model_cd = 'GICS-I'
   AND m.industry_model_id = i.industry_model_id
   AND i.industry_num = #GICS_SUB_INDUSTRY.gics_industry_num

DELETE #GICS_SUB_INDUSTRY
  FROM QER..sub_industry s
 WHERE s.industry_id = #GICS_SUB_INDUSTRY.industry_id
   AND s.sub_industry_num = #GICS_SUB_INDUSTRY.gics_sub_industry_num
   AND s.sub_industry_nm IS NOT NULL

UPDATE QER..sub_industry
   SET sub_industry_nm = g.gics_sub_industry_nm
  FROM #GICS_SUB_INDUSTRY g
 WHERE QER..sub_industry.industry_id = g.industry_id
   AND QER..sub_industry.sub_industry_num = g.gics_sub_industry_num
   AND QER..sub_industry.sub_industry_nm IS NULL

DELETE #GICS_SUB_INDUSTRY
 WHERE gics_sub_industry_num IN (
       SELECT s.sub_industry_num
         FROM QER..industry_model m, QER..industry i, QER..sub_industry s
        WHERE m.industry_model_cd = 'GICS-I'
          AND m.industry_model_id = i.industry_model_id
          AND i.industry_id = s.industry_id)

INSERT QER..sub_industry
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