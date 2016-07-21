use QER
go
IF OBJECT_ID('dbo.us_high_dividend_history_get') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.us_high_dividend_history_get
    IF OBJECT_ID('dbo.us_high_dividend_history_get') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.us_high_dividend_history_get >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.us_high_dividend_history_get >>>'
END
go
CREATE PROCEDURE dbo.us_high_dividend_history_get  
@BDATE1 datetime,  
@BDATE2 datetime,  
@TICKER varchar(32),  
@DEBUG bit = NULL  
AS  
  
DECLARE @BDATE3 datetime,  
        @SECURITY_ID int  
  
IF @BDATE1 > @BDATE2  
BEGIN  
  SELECT @BDATE3 = @BDATE1  
  SELECT @BDATE1 = @BDATE2  
  SELECT @BDATE2 = @BDATE3  
END  
  
SELECT @SECURITY_ID = security_id  
  FROM equity_common..security  
 WHERE ticker = @TICKER  
  
CREATE TABLE #RESULT (  
  bdate     datetime NULL,  
  security_id   int   NULL,  
  sector_id    int   NULL,  
  sector_nm    varchar(64) NULL,  
  industry_id   int   NULL,  
  industry_nm   varchar(64) NULL,  
  ticker    varchar(32) NULL,  
  imnt_nm    varchar(100) NULL,  
  sedol     varchar(32) NULL,  
  cusip     varchar(32) NULL,  
  mkt_cap    float  NULL,  
  div_yield    float  NULL,  
  dps_growth   float  NULL,  
  div_payout_ltm  float  NULL,  
  debt_to_capital  float  NULL,  
  interest_coverage  float  NULL,  
  fcf_ltm_to_div_ltm float  NULL,  
  div_yield_to_5yr_avg float  NULL,  
  pb_to_5yr_avg   float  NULL,  
  sp_current_rating  varchar(4) NULL,  
  sp_senior_rating  varchar(4) NULL  
)  
  
INSERT #RESULT   
      (bdate, security_id, div_yield, dps_growth, div_payout_ltm, debt_to_capital,  
       interest_coverage, fcf_ltm_to_div_ltm, div_yield_to_5yr_avg,  
       pb_to_5yr_avg, sp_current_rating, sp_senior_rating)  
SELECT bdate, security_id, div_yield, dps_growth, div_payout_ltm, debt_to_capital,  
       interest_coverage, fcf_ltm_to_div_ltm, div_yield_to_5yr_avg,  
       pb_to_5yr_avg, sp_current_rating, sp_senior_rating  
  FROM us_high_dividend  
 WHERE security_id = @SECURITY_ID  
   AND bdate >= @BDATE1  
   AND bdate <= @BDATE2  
  
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
   AND #RESULT.bdate = p.reference_date  
  
UPDATE #RESULT  
   SET sector_id = ss.sector_id  
  FROM sector_model_security ss  
 WHERE #RESULT.security_id = ss.security_id  
   AND #RESULT.bdate = ss.bdate  
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
   AND m.industry_model_cd = 'GICS'  
   AND m.industry_model_id = i.industry_model_id  
   AND y.gics_industry_num = i.industry_num  
  
IF @DEBUG = 1  
BEGIN  
  SELECT '#RESULT: FINAL STATE'  
  SELECT * FROM #RESULT ORDER BY security_id  
END  
  
SELECT bdate    AS [Date],  
       sector_nm   AS [Sector GICS Name],  
       industry_nm   AS [Industry GICS Name],  
       ticker    AS [Ticker Symbol],  
       imnt_nm    AS [Company Name],  
       sedol    AS [SEDOL],  
       SUBSTRING(cusip,1,8) AS [CUSIP],  
       mkt_cap    AS [Mkt Cap],  
       div_yield   AS [Div Yield (Indicated)],  
       dps_growth   AS [DPS Growth (5 Yrs)],  
       div_payout_ltm  AS [Div Payout LTM],  
       debt_to_capital  AS [Debt / Capital],  
       interest_coverage AS [Interest Coverage],  
       fcf_ltm_to_div_ltm AS [FCF LTM / Div LTM],  
       div_yield_to_5yr_avg AS [Div Yield / Avg Div Yield],  
       pb_to_5yr_avg  AS [P/B / 5 Yr Avg P/B],  
       sp_current_rating AS [Cur S&P Stock/Credit Rating],  
       sp_senior_rating  AS [S&P Senior Rating]  
  FROM #RESULT  
 ORDER BY bdate DESC, sector_nm, industry_nm, ticker  
  
DROP TABLE #RESULT  
  
RETURN 0  
go
IF OBJECT_ID('dbo.us_high_dividend_history_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.us_high_dividend_history_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.us_high_dividend_history_get >>>'
go
