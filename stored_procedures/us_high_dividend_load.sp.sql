use QER
go
IF OBJECT_ID('dbo.us_high_dividend_load') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.us_high_dividend_load
    IF OBJECT_ID('dbo.us_high_dividend_load') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.us_high_dividend_load >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.us_high_dividend_load >>>'
END
go
CREATE PROCEDURE dbo.us_high_dividend_load
@BDATE datetime,
@DELETE_FLAG bit = 0,
@DEBUG bit = NULL
AS

CREATE TABLE #USHD_SECURITY_ID (
  security_id	int				NULL,
  ticker		varchar(32)		NULL,
  cusip			varchar(32)		NULL,
  sedol			varchar(32)		NULL,
  company_nm	varchar(100)	NULL
)

INSERT #USHD_SECURITY_ID
SELECT NULL, ticker, cusip, sedol, company_nm
  FROM us_high_dividend_staging

IF @DEBUG = 1
BEGIN
  SELECT '#USHD_SECURITY_ID: AFTER INITIAL INSERT'
  SELECT * FROM #USHD_SECURITY_ID ORDER BY cusip, sedol
END

UPDATE #USHD_SECURITY_ID
   SET cusip = equity_common.dbo.fnCusipIncludeCheckDigit(cusip)

UPDATE #USHD_SECURITY_ID
   SET sedol = equity_common.dbo.fnSedolIncludeCheckDigit(sedol)

IF @DEBUG = 1
BEGIN
  SELECT '#USHD_SECURITY_ID: AFTER CUSIP AND SEDOL UPDATE'
  SELECT * FROM #USHD_SECURITY_ID ORDER BY cusip, sedol
END

UPDATE #USHD_SECURITY_ID
   SET security_id = y.security_id
  FROM equity_common..security y
 WHERE #USHD_SECURITY_ID.cusip = y.cusip
   AND #USHD_SECURITY_ID.sedol = y.sedol

UPDATE #USHD_SECURITY_ID
   SET security_id = y.security_id
  FROM equity_common..security y
 WHERE #USHD_SECURITY_ID.cusip = y.cusip
   AND #USHD_SECURITY_ID.ticker = y.ticker
   AND #USHD_SECURITY_ID.security_id IS NULL

UPDATE #USHD_SECURITY_ID
   SET security_id = y.security_id
  FROM equity_common..security y
 WHERE #USHD_SECURITY_ID.cusip = y.cusip
   AND #USHD_SECURITY_ID.security_id IS NULL

UPDATE #USHD_SECURITY_ID
   SET security_id = y.security_id
  FROM equity_common..security y
 WHERE #USHD_SECURITY_ID.ticker = y.ticker
   AND #USHD_SECURITY_ID.security_id IS NULL

IF @DEBUG = 1
BEGIN
  SELECT '#USHD_SECURITY_ID: AFTER SECURITY_ID UPDATE'
  SELECT * FROM #USHD_SECURITY_ID ORDER BY cusip, sedol
END

IF @DELETE_FLAG = 1
BEGIN
  DELETE us_high_dividend
   WHERE bdate = @BDATE
END
ELSE
BEGIN
  DELETE us_high_dividend
    FROM #USHD_SECURITY_ID i, us_high_dividend_staging s
   WHERE us_high_dividend.bdate = @BDATE
     AND us_high_dividend.security_id = i.security_id
     AND i.cusip = equity_common.dbo.fnCusipIncludeCheckDigit(s.cusip)
     AND i.sedol = equity_common.dbo.fnSedolIncludeCheckDigit(s.sedol)
END

INSERT us_high_dividend
SELECT @BDATE, i.security_id,
       s.div_yield,
       s.dps_growth,
       s.div_payout_ltm,
       s.debt_to_capital,
       s.interest_coverage,
       s.fcf_ltm_to_div_ltm,
       s.div_yield_to_5yr_avg,
       s.pb_to_5yr_avg,
       s.sp_current_rating,
       s.sp_senior_rating
  FROM #USHD_SECURITY_ID i, us_high_dividend_staging s
 WHERE i.security_id IS NOT NULL
   AND i.cusip = equity_common.dbo.fnCusipIncludeCheckDigit(s.cusip)
   AND i.sedol = equity_common.dbo.fnSedolIncludeCheckDigit(s.sedol)

DROP TABLE #USHD_SECURITY_ID

RETURN 0
go
IF OBJECT_ID('dbo.us_high_dividend_load') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.us_high_dividend_load >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.us_high_dividend_load >>>'
go
