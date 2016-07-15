use QER
go
IF OBJECT_ID('dbo.currency_decode_update') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.currency_decode_update
    IF OBJECT_ID('dbo.currency_decode_update') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.currency_decode_update >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.currency_decode_update >>>'
END
go
CREATE PROCEDURE dbo.currency_decode_update
AS

CREATE TABLE #CURRENCY (
  currency_cd	varchar(8)	NULL,
  currency_nm	varchar(128)	NULL
)

INSERT #CURRENCY
SELECT DISTINCT upper(currency_local), upper(currency_nm)
  FROM QER..instrument_characteristics_staging
 WHERE currency_local IS NOT NULL
   AND currency_nm IS NOT NULL

DELETE #CURRENCY
  FROM QER..decode d
 WHERE d.item = 'CURRENCY'
   AND #CURRENCY.currency_cd = d.code
   AND #CURRENCY.currency_nm = d.decode

DELETE QER..decode
  FROM #CURRENCY c
 WHERE QER..decode.item = 'CURRENCY'
   AND QER..decode.code = c.currency_cd

INSERT QER..decode
SELECT 'CURRENCY', currency_cd, currency_nm
  FROM #CURRENCY

DROP TABLE #CURRENCY

RETURN 0
go
IF OBJECT_ID('dbo.currency_decode_update') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.currency_decode_update >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.currency_decode_update >>>'
go