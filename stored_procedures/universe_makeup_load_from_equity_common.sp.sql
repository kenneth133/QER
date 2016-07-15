use QER
go
IF OBJECT_ID('dbo.universe_makeup_load_from_equity_common') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.universe_makeup_load_from_equity_common
    IF OBJECT_ID('dbo.universe_makeup_load_from_equity_common') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.universe_makeup_load_from_equity_common >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.universe_makeup_load_from_equity_common >>>'
END
go
CREATE PROCEDURE dbo.universe_makeup_load_from_equity_common @UNIVERSE_CD varchar(32) = NULL,
                                                             @DATE datetime = NULL
AS

IF @UNIVERSE_CD IS NOT NULL AND NOT EXISTS (SELECT * FROM decode WHERE item = 'EQUITY_COMMON_BM' AND code = @UNIVERSE_CD)
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @UNIVERSE_CD PARAMETER' RETURN -1 END

IF @DATE IS NULL
  BEGIN EXEC business_date_get @DIFF=-1, @RET_DATE=@DATE OUTPUT END

CREATE TABLE #POS (
  universe_cd		varchar(32)		NOT NULL,
  acct_cd			varchar(50)		NOT NULL,
  security_id		int				NOT NULL,
  quantity			float			NOT NULL,
  price_close_usd	float			NULL,
  mkt_cap			float			NULL,
  weight			float			NULL
)

IF @UNIVERSE_CD IS NOT NULL
BEGIN
  DELETE universe_makeup
    FROM universe_def d
   WHERE universe_makeup.universe_id = d.universe_id
     AND d.universe_cd = @UNIVERSE_CD
     AND universe_makeup.universe_dt = @DATE

  INSERT #POS
        (universe_cd, acct_cd, security_id, quantity, price_close_usd)
  SELECT @UNIVERSE_CD, p.acct_cd, p.security_id, p.quantity, m.price_close_usd
    FROM decode d, equity_common..position p, equity_common..market_price m
   WHERE d.item = 'EQUITY_COMMON_BM'
     AND d.code = @UNIVERSE_CD
     AND d.decode = p.acct_cd
     AND p.reference_date = @DATE
     AND p.reference_date = p.effective_date
     AND m.reference_date = p.reference_date
     AND m.security_id = p.security_id
END
ELSE
BEGIN
  DELETE universe_makeup
    FROM universe_def d, decode c
   WHERE c.item = 'EQUITY_COMMON_BM'
     AND c.code = d.universe_cd
     AND universe_makeup.universe_id = d.universe_id
     AND universe_makeup.universe_dt = @DATE

  INSERT #POS
        (universe_cd, acct_cd, security_id, quantity, price_close_usd)
  SELECT d.code, p.acct_cd, p.security_id, p.quantity, m.price_close_usd
    FROM decode d, equity_common..position p, equity_common..market_price m
   WHERE d.item = 'EQUITY_COMMON_BM'
     AND d.decode = p.acct_cd
     AND p.reference_date = @DATE
     AND p.reference_date = p.effective_date
     AND m.reference_date = p.reference_date
     AND m.security_id = p.security_id
END

UPDATE #POS SET quantity = 0.0 WHERE quantity IS NULL
UPDATE #POS SET price_close_usd = 0.0 WHERE price_close_usd IS NULL
UPDATE #POS SET mkt_cap = quantity * price_close_usd

UPDATE #POS
   SET weight = (mkt_cap / x.total_mkt_cap) * 100.0
  FROM (SELECT universe_cd, SUM(mkt_cap) AS [total_mkt_cap] FROM #POS
         GROUP BY universe_cd) x
 WHERE #POS.universe_cd = x.universe_cd
   AND x.total_mkt_cap != 0.0

UPDATE #POS SET weight = 0.0 WHERE weight IS NULL

INSERT universe_makeup
      (universe_dt, universe_id, ticker, cusip, sedol, isin, weight)
SELECT @DATE, d.universe_id, s.ticker, SUBSTRING(s.cusip, 1, 8), s.sedol, s.isin, p.weight
  FROM #POS p, universe_def d, equity_common..security s
 WHERE p.universe_cd = d.universe_cd
   AND p.security_id = s.security_id

UPDATE universe_makeup
   SET mqa_id = i.mqa_id,
       gv_key = i.gv_key
  FROM universe_def d, instrument_characteristics i
 WHERE d.universe_cd IN (SELECT DISTINCT universe_cd FROM #POS)
   AND universe_makeup.universe_id = d.universe_id
   AND universe_makeup.universe_dt = @DATE
   AND universe_makeup.universe_dt = i.bdate
   AND universe_makeup.cusip = i.cusip

DROP TABLE #POS

RETURN 0
go
IF OBJECT_ID('dbo.universe_makeup_load_from_equity_common') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.universe_makeup_load_from_equity_common >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.universe_makeup_load_from_equity_common >>>'
go
