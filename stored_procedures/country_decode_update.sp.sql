use QER
go
IF OBJECT_ID('dbo.country_decode_update') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.country_decode_update
    IF OBJECT_ID('dbo.country_decode_update') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.country_decode_update >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.country_decode_update >>>'
END
go
CREATE PROCEDURE dbo.country_decode_update
AS

CREATE TABLE #COUNTRY (
  country	varchar(8)	NULL,
  ctry_name	varchar(128)	NULL
)

INSERT #COUNTRY
SELECT DISTINCT upper(country), upper(ctry_name)
  FROM QER..instrument_characteristics_staging
 WHERE country IS NOT NULL
   AND ctry_name IS NOT NULL

DELETE #COUNTRY
  FROM QER..decode d
 WHERE d.item = 'COUNTRY'
   AND #COUNTRY.country = d.code
   AND #COUNTRY.ctry_name = d.decode

DELETE QER..decode
  FROM #COUNTRY e
 WHERE QER..decode.item = 'COUNTRY'
   AND QER..decode.code = e.country

INSERT QER..decode
SELECT 'COUNTRY', country, ctry_name
  FROM #COUNTRY

DROP TABLE #COUNTRY

RETURN 0
go
IF OBJECT_ID('dbo.country_decode_update') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.country_decode_update >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.country_decode_update >>>'
go