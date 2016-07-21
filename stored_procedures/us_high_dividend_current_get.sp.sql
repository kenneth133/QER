use QER
go
IF OBJECT_ID('dbo.us_high_dividend_current_get') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.us_high_dividend_current_get
    IF OBJECT_ID('dbo.us_high_dividend_current_get') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.us_high_dividend_current_get >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.us_high_dividend_current_get >>>'
END
go
CREATE PROCEDURE dbo.us_high_dividend_current_get
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
  eq_div_flg		bit				NULL,

  sector_id			int				NULL,
  sector_num		int				NULL,
  sector_nm			varchar(64)		NULL,
  industry_id		int				NULL,
  industry_num		int				NULL,
  industry_nm		varchar(64)		NULL,

  total_score		float		NULL,

  mkt_cap			float		NULL,
  units				float		NULL,
  price				float		NULL,
  mval				float		NULL,

  account_wgt		float		NULL,
  benchmark_wgt		float		NULL,
  active_wgt		float		NULL,

  div_yield				float		NULL,
  dps_growth			float		NULL,
  div_payout_ltm		float		NULL,
  debt_to_capital		float		NULL,
  interest_coverage		float		NULL,
  fcf_ltm_to_div_ltm	float		NULL,
  div_yield_to_5yr_avg	float		NULL,
  pb_to_5yr_avg			float		NULL,
  sp_current_rating		varchar(4)	NULL,
  sp_senior_rating		varchar(4)	NULL
)

INSERT #RESULT (security_id, eq_div_flg)
SELECT h.security_id, 0
  FROM us_high_dividend h, universe_def d, universe_makeup p
 WHERE d.universe_cd = 'USHD'
   AND d.universe_id = p.universe_id
   AND p.universe_dt = @BDATE
   AND h.bdate = @BDATE
   AND h.security_id = p.security_id
UNION
SELECT security_id, 0 FROM equity_common..position
 WHERE reference_date = @BDATE
   AND reference_date = effective_date
   AND acct_cd IN (SELECT code FROM decode WHERE item = 'US HIGH DIVIDEND REPORT')
UNION
SELECT security_id, 0 FROM equity_common..benchmark_weight
 WHERE reference_date = @BDATE
   AND reference_date = effective_date
   AND acct_cd IN (SELECT decode FROM decode WHERE item = 'US HIGH DIVIDEND REPORT')

UPDATE #RESULT
   SET eq_div_flg = 1
  FROM universe_def d, universe_makeup p
 WHERE d.universe_cd = 'USHD'
   AND d.universe_id = p.universe_id
   AND p.universe_dt = @BDATE
   AND #RESULT.security_id = p.security_id

UPDATE #RESULT
   SET ticker = y.ticker,
       cusip = y.cusip,
       sedol = y.sedol,
       isin = y.isin,
       imnt_nm = y.security_name,
       sector_num = y.gics_sector_num,
       industry_num = y.gics_industry_num
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
   SET industry_id = i.industry_id,
       industry_nm = i.industry_nm
  FROM industry_model m, industry i
 WHERE m.industry_model_cd = 'GICS'
   AND m.industry_model_id = i.industry_model_id
   AND #RESULT.industry_num = i.industry_num

UPDATE #RESULT
   SET total_score = s.total_score
  FROM scores s
 WHERE s.bdate = @BDATE
   AND s.strategy_id IN (SELECT strategy_id FROM strategy WHERE strategy_cd LIKE 'LCR-%')
   AND #RESULT.security_id = s.security_id

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
           AND acct_cd IN (SELECT code FROM decode WHERE item = 'US HIGH DIVIDEND REPORT')
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
   AND w.acct_cd IN (SELECT decode FROM decode WHERE item = 'US HIGH DIVIDEND REPORT')
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
   SET div_yield = d.div_yield,
       dps_growth = d.dps_growth,
       div_payout_ltm = d.div_payout_ltm,
       debt_to_capital = d.debt_to_capital,
       interest_coverage = d.interest_coverage,
       fcf_ltm_to_div_ltm = d.fcf_ltm_to_div_ltm,
       div_yield_to_5yr_avg = d.div_yield_to_5yr_avg,
       pb_to_5yr_avg = d.pb_to_5yr_avg,
       sp_current_rating = d.sp_current_rating,
       sp_senior_rating = d.sp_senior_rating
  FROM us_high_dividend d
 WHERE d.bdate = @BDATE
   AND #RESULT.security_id = d.security_id

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT: FINAL STATE'
  SELECT * FROM #RESULT ORDER BY security_id
END

SELECT sector_nm			AS [Sector GICS Name],
       industry_nm			AS [Industry GICS Name],
       ticker				AS [Ticker Symbol],
       imnt_nm				AS [Company Name],
       sedol				AS [SEDOL],
       SUBSTRING(cusip,1,8)	AS [CUSIP],
       CASE eq_div_flg WHEN 1 THEN '*' ELSE '' END AS [Asterisk],
       total_score			AS [Rank],
       mkt_cap				AS [Mkt Cap],
       account_wgt			AS [Portfolio Wgt],
       benchmark_wgt		AS [Benchmark Wgt],
       active_wgt			AS [Active Wgt],
       div_yield			AS [Div Yield (Indicated)],
       dps_growth			AS [DPS Growth (5 Yrs)],
       div_payout_ltm		AS [Div Payout LTM],
       debt_to_capital		AS [Debt / Capital],
       interest_coverage	AS [Interest Coverage],
       fcf_ltm_to_div_ltm	AS [FCF LTM / Div LTM],
       div_yield_to_5yr_avg	AS [Div Yield / Avg Div Yield],
       pb_to_5yr_avg		AS [P/B / 5 Yr Avg P/B],
       sp_current_rating	AS [Cur S&P Stock/Credit Rating],
       sp_senior_rating		AS [S&P Senior Rating]
  FROM #RESULT
 ORDER BY sector_nm, industry_nm, active_wgt DESC, ticker

DROP TABLE #RESULT

RETURN 0
go
IF OBJECT_ID('dbo.us_high_dividend_current_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.us_high_dividend_current_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.us_high_dividend_current_get >>>'
go
