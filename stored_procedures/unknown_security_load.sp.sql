use QER
go
IF OBJECT_ID('dbo.unknown_security_load') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.unknown_security_load
    IF OBJECT_ID('dbo.unknown_security_load') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.unknown_security_load >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.unknown_security_load >>>'
END
go
CREATE PROCEDURE dbo.unknown_security_load @BDATE datetime = NULL
AS

DECLARE @NOW datetime
SELECT @NOW = GETDATE()

IF @BDATE IS NULL
BEGIN
  EXEC QER..business_date_get @DIFF=-1, @RET_DATE=@BDATE OUTPUT
END

INSERT decode
SELECT DISTINCT 'UNKNOWN SECURITY', 'CUSIP', p.cusip
  FROM position p
 WHERE p.bdate = @BDATE
   AND p.account_cd IN (SELECT code FROM decode WHERE item = 'UNKNOWN SECURITY ACCOUNT')
   AND p.cusip NOT IN (SELECT decode FROM decode WHERE item = 'UNKNOWN SECURITY' AND code = 'CUSIP')
   AND NOT EXISTS (SELECT 1 FROM instrument_characteristics i
                    WHERE i.bdate = @BDATE
                      AND i.cusip = p.cusip)

CREATE TABLE #SECURITY (
  security_id		int				NULL,
  identifier_type	varchar(16)		NOT NULL,
  identifier_value	varchar(64)		NOT NULL,
  price_close		float			NULL,
  price_close_usd	float			NULL,
  volume			float			NULL,
  market_cap		float			NULL
)

INSERT #SECURITY (identifier_type, identifier_value)
SELECT d.code, d.decode
  FROM decode d
 WHERE d.item = 'UNKNOWN SECURITY'
   AND d.code = 'TICKER'
   AND EXISTS (SELECT 1 FROM position p
                WHERE p.bdate = @BDATE
                  AND p.ticker = d.decode)
   AND NOT EXISTS (SELECT 1 FROM instrument_characteristics i
                    WHERE i.bdate = @BDATE
                      AND i.ticker = d.decode)

INSERT #SECURITY (identifier_type, identifier_value)
SELECT d.code, d.decode + '%'
  FROM decode d
 WHERE d.item = 'UNKNOWN SECURITY'
   AND d.code = 'CUSIP'
   AND EXISTS (SELECT 1 FROM position p
                WHERE p.bdate = @BDATE
                  AND p.cusip = d.decode)
   AND NOT EXISTS (SELECT 1 FROM instrument_characteristics i
                    WHERE i.bdate = @BDATE
                      AND i.cusip = d.decode)

INSERT #SECURITY (identifier_type, identifier_value)
SELECT d.code, d.decode + '%'
  FROM decode d
 WHERE d.item = 'UNKNOWN SECURITY'
   AND d.code = 'SEDOL'
   AND EXISTS (SELECT 1 FROM position p
                WHERE p.bdate = @BDATE
                  AND p.sedol = d.decode)
   AND NOT EXISTS (SELECT 1 FROM instrument_characteristics i
                    WHERE i.bdate = @BDATE
                      AND i.sedol = d.decode)

INSERT #SECURITY (identifier_type, identifier_value)
SELECT d.code, d.decode
  FROM decode d
 WHERE d.item = 'UNKNOWN SECURITY'
   AND d.code = 'ISIN'
   AND EXISTS (SELECT 1 FROM position p
                WHERE p.bdate = @BDATE
                  AND p.isin = d.decode)
   AND NOT EXISTS (SELECT 1 FROM instrument_characteristics i
                    WHERE i.bdate = @BDATE
                      AND i.isin = d.decode)

UPDATE #SECURITY
   SET security_id = s.security_id
  FROM equity_common..security s
 WHERE #SECURITY.identifier_type = 'TICKER'
   AND #SECURITY.identifier_value = s.ticker

UPDATE #SECURITY
   SET security_id = s.security_id
  FROM equity_common..security s
 WHERE #SECURITY.identifier_type = 'CUSIP'
   AND s.cusip LIKE #SECURITY.identifier_value

UPDATE #SECURITY
   SET security_id = s.security_id
  FROM equity_common..security s
 WHERE #SECURITY.identifier_type = 'SEDOL'
   AND s.sedol LIKE #SECURITY.identifier_value

UPDATE #SECURITY
   SET security_id = s.security_id
  FROM equity_common..security s
 WHERE #SECURITY.identifier_type = 'ISIN'
   AND #SECURITY.identifier_value = s.isin

UPDATE #SECURITY
   SET price_close = p.price_close,
       price_close_usd = p.price_close_usd,
       volume = p.volume,
       market_cap = p.market_cap / 1000000.0
  FROM equity_common..market_price p
 WHERE p.reference_date = @BDATE
   AND p.security_id = #SECURITY.security_id

INSERT instrument_characteristics
      (bdate, ticker, cusip, sedol, isin, imnt_nm,
       price_close, price_close_local, currency_local, mkt_cap, volume,
       gics_sector_num, gics_segment_num, gics_industry_num, gics_sub_industry_num,
       russell_sector_num, russell_industry_num, update_tm, source_cd)
SELECT @BDATE, s.ticker, SUBSTRING(s.cusip,1,8), s.sedol, s.isin, s.security_name,
       t.price_close_usd, t.price_close, s.local_ccy_cd, t.market_cap, t.volume,
       s.gics_sector_num, CONVERT(int,SUBSTRING(CONVERT(varchar,s.gics_industry_num),1,4)),
       s.gics_industry_num, s.gics_sub_industry_num,
       s.russell_sector_num, s.russell_industry_num, @NOW, 'FS'
  FROM #SECURITY t, equity_common..security s
 WHERE t.security_id = s.security_id

DROP TABLE #SECURITY

RETURN 0
go
IF OBJECT_ID('dbo.unknown_security_load') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.unknown_security_load >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.unknown_security_load >>>'
go
