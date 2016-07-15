use QER
go
IF OBJECT_ID('dbo.exchange_decode_update') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.exchange_decode_update
    IF OBJECT_ID('dbo.exchange_decode_update') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.exchange_decode_update >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.exchange_decode_update >>>'
END
go
CREATE PROCEDURE dbo.exchange_decode_update
AS

CREATE TABLE #EXCHANGE (
  exchange	varchar(4)	NULL,
  dexchange	varchar(128)	NULL
)

INSERT #EXCHANGE
SELECT DISTINCT upper(exchange), upper(dexchange)
  FROM QER..instrument_characteristics_staging
 WHERE exchange IS NOT NULL
   AND dexchange IS NOT NULL

DELETE #EXCHANGE
  FROM QER..decode d
 WHERE d.item = 'EXCHANGE'
   AND #EXCHANGE.exchange = d.code
   AND #EXCHANGE.dexchange = d.decode

DELETE QER..decode
  FROM #EXCHANGE e
 WHERE QER..decode.item = 'EXCHANGE'
   AND QER..decode.code = e.exchange

INSERT QER..decode
SELECT 'EXCHANGE', exchange, dexchange
  FROM #EXCHANGE

DROP TABLE #EXCHANGE

RETURN 0
go
IF OBJECT_ID('dbo.exchange_decode_update') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.exchange_decode_update >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.exchange_decode_update >>>'
go