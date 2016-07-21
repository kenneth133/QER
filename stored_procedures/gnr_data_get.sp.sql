use QER
go
IF OBJECT_ID('dbo.gnr_data_get') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.gnr_data_get
    IF OBJECT_ID('dbo.gnr_data_get') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.gnr_data_get >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.gnr_data_get >>>'
END
go
CREATE PROCEDURE dbo.gnr_data_get
@BDATE datetime,
@DEBUG bit = NULL
AS

CREATE TABLE #RESULT (
  security_id		int				NULL,
  ticker			varchar(32)		NULL,
  cusip				varchar(32)		NULL,
  sedol				varchar(32)		NULL,
  isin				varchar(32)		NULL,
  imnt_nm			varchar(100)	NULL,

  sector_id			int				NULL,
  sector_num		int				NULL,
  sector_nm			varchar(64)		NULL,
  sub_industry_num	int				NULL,
  sub_industry_nm	varchar(64)		NULL,

  mkt_cap			float		NULL,
  units				float		NULL,
  price				float		NULL,
  mval				float		NULL,

  account_wgt		float		NULL,
  benchmark_wgt		float		NULL,
  active_wgt		float		NULL,

  total_score			float		NULL,
  div_yield_to_5yr_avg	float		NULL,
  pb_to_5yr_avg			float		NULL
)

INSERT #RESULT (security_id)
SELECT security_id FROM equity_common..position
 WHERE reference_date = @BDATE
   AND reference_date = effective_date
   AND acct_cd IN (SELECT code FROM decode WHERE item = 'GLOBAL NATURAL RESOURCES REPORT' AND code != 'STRATEGY_CD')
UNION
SELECT security_id FROM equity_common..benchmark_weight
 WHERE reference_date = @BDATE
   AND reference_date = effective_date
   AND acct_cd IN (SELECT decode FROM decode WHERE item = 'GLOBAL NATURAL RESOURCES REPORT' AND code != 'STRATEGY_CD')

UPDATE #RESULT
   SET ticker = y.ticker,
       cusip = y.cusip,
       sedol = y.sedol,
       isin = y.isin,
       imnt_nm = y.security_name,
       sector_num = y.gics_sector_num,
       sub_industry_num = y.gics_sub_industry_num
  FROM equity_common..security y
 WHERE #RESULT.security_id = y.security_id

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (1)'
  SELECT * FROM #RESULT ORDER BY security_id
END

UPDATE #RESULT
   SET sector_id = d.sector_id,
       sector_nm = d.sector_nm
  FROM sector_model m, sector_def d
 WHERE m.sector_model_cd = 'GICS-S'
   AND m.sector_model_id = d.sector_model_id
   AND #RESULT.sector_num = d.sector_num

UPDATE #RESULT
   SET sub_industry_nm = b.sub_industry_nm
  FROM industry_model m, industry i, sub_industry b
 WHERE m.industry_model_cd = 'GICS'
   AND m.industry_model_id = i.industry_model_id
   AND i.industry_id = b.industry_id
   AND b.sub_industry_num = #RESULT.sub_industry_num

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (2)'
  SELECT * FROM #RESULT ORDER BY security_id
END

UPDATE #RESULT
   SET mkt_cap = p.market_cap / 1000000.0,
       price = p.price_close_usd
  FROM equity_common..market_price p
 WHERE #RESULT.security_id = p.security_id
   AND p.reference_date = @BDATE

UPDATE #RESULT
   SET units = x.quantity
  FROM (SELECT security_id, SUM(ISNULL(quantity,0.0)) AS [quantity]
          FROM equity_common..position
         WHERE reference_date = @BDATE
           AND reference_date = effective_date
           AND acct_cd IN (SELECT code FROM decode WHERE item = 'GLOBAL NATURAL RESOURCES REPORT' AND code != 'STRATEGY_CD')
         GROUP BY security_id) x
 WHERE #RESULT.security_id = x.security_id

UPDATE #RESULT
   SET units = 0.0
 WHERE units IS NULL

UPDATE #RESULT
   SET mval = units * price

DECLARE @ACCOUNT_MVAL float
SELECT @ACCOUNT_MVAL = SUM(mval) FROM #RESULT

IF @ACCOUNT_MVAL != 0.0
  BEGIN UPDATE #RESULT SET account_wgt = mval / @ACCOUNT_MVAL END

UPDATE #RESULT
   SET account_wgt = 0.0
 WHERE account_wgt IS NULL

UPDATE #RESULT
   SET benchmark_wgt = w.weight
  FROM equity_common..benchmark_weight w
 WHERE w.reference_date = @BDATE
   AND w.reference_date = w.effective_date
   AND w.acct_cd IN (SELECT decode FROM decode WHERE item = 'GLOBAL NATURAL RESOURCES REPORT' AND code != 'STRATEGY_CD')
   AND #RESULT.security_id = w.security_id

UPDATE #RESULT
   SET benchmark_wgt = 0.0
 WHERE benchmark_wgt IS NULL

UPDATE #RESULT
   SET active_wgt = account_wgt - benchmark_wgt

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (3)'
  SELECT * FROM #RESULT ORDER BY security_id
END

UPDATE #RESULT
   SET total_score = s.total_score
  FROM strategy g, scores s
 WHERE s.bdate = @BDATE
   AND s.strategy_id = g.strategy_id
   AND g.strategy_cd IN (SELECT decode FROM decode WHERE item = 'GLOBAL NATURAL RESOURCES REPORT' AND code = 'STRATEGY_CD')
   AND #RESULT.security_id = s.security_id

UPDATE #RESULT
   SET div_yield_to_5yr_avg = d.div_yield_to_5yr_avg,
       pb_to_5yr_avg = d.pb_to_5yr_avg
  FROM us_high_dividend d
 WHERE d.bdate = @BDATE
   AND #RESULT.security_id = d.security_id

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT: FINAL STATE'
  SELECT * FROM #RESULT ORDER BY security_id
END

SELECT sector_nm			AS [Sector GICS Name],
       sub_industry_nm		AS [Sub-Industry GICS Name],
       ticker				AS [Ticker Symbol],
       imnt_nm				AS [Company Name],
       sedol				AS [SEDOL],
       SUBSTRING(cusip,1,8)	AS [CUSIP],
       mkt_cap				AS [Mkt Cap],
       account_wgt			AS [Portfolio Wgt],
       benchmark_wgt		AS [Benchmark Wgt],
       active_wgt			AS [Active Wgt],
       total_score			AS [Total Rank],
       div_yield_to_5yr_avg	AS [Div Yield / Avg Div Yield],
       pb_to_5yr_avg		AS [P/B / 5 Yr Avg P/B]
  FROM #RESULT
 ORDER BY sector_nm, sub_industry_nm, active_wgt DESC, ticker

DROP TABLE #RESULT

RETURN 0
go
IF OBJECT_ID('dbo.gnr_data_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.gnr_data_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.gnr_data_get >>>'
go
