use QER
go
IF OBJECT_ID('dbo.sec_type_decode_update') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.sec_type_decode_update
    IF OBJECT_ID('dbo.sec_type_decode_update') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.sec_type_decode_update >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.sec_type_decode_update >>>'
END
go
CREATE PROCEDURE dbo.sec_type_decode_update
AS

CREATE TABLE #SEC_TYPE (
  sectype	varchar(4)	NULL,
  dsectype	varchar(128)	NULL
)

INSERT #SEC_TYPE
SELECT DISTINCT upper(sectype), upper(dsectype)
  FROM QER..instrument_characteristics_staging
 WHERE sectype IS NOT NULL
   AND dsectype IS NOT NULL

DELETE #SEC_TYPE
  FROM QER..decode d
 WHERE d.item = 'SEC_TYPE'
   AND #SEC_TYPE.sectype = d.code
   AND #SEC_TYPE.dsectype = d.decode

DELETE QER..decode
  FROM #SEC_TYPE s
 WHERE QER..decode.item = 'SEC_TYPE'
   AND QER..decode.code = s.sectype

INSERT QER..decode
SELECT 'SEC_TYPE', sectype, dsectype
  FROM #SEC_TYPE

DROP TABLE #SEC_TYPE

RETURN 0
go
IF OBJECT_ID('dbo.sec_type_decode_update') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.sec_type_decode_update >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.sec_type_decode_update >>>'
go