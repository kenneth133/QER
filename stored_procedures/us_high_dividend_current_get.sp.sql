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
  security_id			int			NULL,
  sector_id				int			NULL,
  sector_nm				varchar(64)	NULL,
  industry_id			int			NULL,
  industry_nm			varchar(64)	NULL,
  ticker				varchar(32)	NULL,
  imnt_nm				varchar(100) NULL,
  sedol					varchar(32)	NULL,
  cusip					varchar(32)	NULL,
  mkt_cap				float		NULL,
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

INSERT #RESULT 
      (security_id, div_yield, dps_growth, div_payout_ltm, debt_to_capital,
       interest_coverage, fcf_ltm_to_div_ltm, div_yield_to_5yr_avg,
       pb_to_5yr_avg, sp_current_rating, sp_senior_rating)
SELECT security_id, div_yield, dps_growth, div_payout_ltm, debt_to_capital,
       interest_coverage, fcf_ltm_to_div_ltm, div_yield_to_5yr_avg,
       pb_to_5yr_avg, sp_current_rating, sp_senior_rating
  FROM us_high_dividend
 WHERE bdate = @BDATE

UPDATE #RESULT
   SET ticker = y.ticker,
       imnt_nm = y.security_name,
       sedol = y.sedol,
       cusip = y.cusip
  FROM equity_common..security y
 WHERE #RESULT.security_id = y.security_id

UPDATE #RESULT
   SET mkt_cap = p.market_cap / 1000000.0
  FROM equity_common..market_price p
 WHERE #RESULT.security_id = p.security_id
   AND p.reference_date = @BDATE

UPDATE #RESULT
   SET sector_id = ss.sector_id
  FROM sector_model_security ss
 WHERE #RESULT.security_id = ss.security_id
   AND ss.bdate = @BDATE
   AND ss.sector_model_id = (SELECT sector_model_id FROM sector_model WHERE sector_model_cd = 'GICS-S')

UPDATE #RESULT
   SET sector_nm = d.sector_nm
  FROM sector_def d
 WHERE #RESULT.sector_id = d.sector_id

UPDATE #RESULT
   SET industry_id = i.industry_id,
       industry_nm = i.industry_nm
  FROM equity_common..security y, industry_model m, industry i
 WHERE #RESULT.security_id = y.security_id
   AND m.industry_model_cd = 'GICS-I'
   AND m.industry_model_id = i.industry_model_id
   AND y.gics_industry_num = i.industry_num

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
       mkt_cap				AS [Mkt Cap],
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
 ORDER BY sector_nm, industry_nm, ticker

DROP TABLE #RESULT

RETURN 0
go
IF OBJECT_ID('dbo.us_high_dividend_current_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.us_high_dividend_current_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.us_high_dividend_current_get >>>'
go
