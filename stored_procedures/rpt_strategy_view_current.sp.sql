use QER
go
IF OBJECT_ID('dbo.rpt_strategy_view_current') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rpt_strategy_view_current
    IF OBJECT_ID('dbo.rpt_strategy_view_current') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rpt_strategy_view_current >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rpt_strategy_view_current >>>'
END
go
CREATE PROCEDURE dbo.rpt_strategy_view_current @BDATE datetime,
                                               @MODEL_TYPE varchar(16),
                                               @IDENTIFIER_TYPE varchar(32),
                                               @IDENTIFIER_VALUE varchar(64),
                                               @DEBUG bit = NULL
AS
/* STOCK - BY STRATEGY */

IF @BDATE IS NULL
  BEGIN SELECT 'ERROR: @BDATE IS A REQUIRED PARAMETER' RETURN -1 END
IF @MODEL_TYPE IS NULL
  BEGIN SELECT 'ERROR: @MODEL_TYPE IS A REQUIRED PARAMETER' RETURN -1 END
IF @MODEL_TYPE NOT IN ('EQUAL', 'CAP')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @MODEL_TYPE PARAMETER' RETURN -1 END
IF @IDENTIFIER_TYPE IS NULL
  BEGIN SELECT 'ERROR: @IDENTIFIER_TYPE IS A REQUIRED PARAMETER' RETURN -1 END
IF @IDENTIFIER_TYPE NOT IN ('TICKER', 'CUSIP', 'SEDOL')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @IDENTIFIER_TYPE PARAMETER' RETURN -1 END
IF @IDENTIFIER_VALUE IS NULL
  BEGIN SELECT 'ERROR: @IDENTIFIER_VALUE IS A REQUIRED PARAMETER' RETURN -1 END

IF @MODEL_TYPE = 'EQUAL'
  BEGIN SELECT @MODEL_TYPE = '_MPF_EQL' END
ELSE IF @MODEL_TYPE = 'CAP'
  BEGIN SELECT @MODEL_TYPE = '_MPF_CAP' END

CREATE TABLE #SECURITY (  
  mqa_id	varchar(32)		NULL,
  ticker	varchar(16)		NULL,
  cusip		varchar(32)		NULL,
  sedol		varchar(32)		NULL,
  isin		varchar(64)		NULL,
  imnt_nm	varchar(255)		NULL,
  country_cd	varchar(4)		NULL,
  country_nm	varchar(128)	NULL
)

IF @IDENTIFIER_TYPE = 'TICKER'  
BEGIN  
  INSERT #SECURITY  
        (mqa_id, ticker, cusip, sedol, isin, imnt_nm, country_cd)  
  SELECT i.mqa_id, i.ticker, i.cusip, i.sedol, i.isin, i.imnt_nm, i.country
    FROM instrument_characteristics i
   WHERE i.bdate = @BDATE  
     AND i.ticker = @IDENTIFIER_VALUE  
END  
IF @IDENTIFIER_TYPE = 'CUSIP'  
BEGIN  
  INSERT #SECURITY  
        (mqa_id, ticker, cusip, sedol, isin, imnt_nm, country_cd)  
  SELECT i.mqa_id, i.ticker, i.cusip, i.sedol, i.isin, i.imnt_nm, i.country
    FROM instrument_characteristics i
   WHERE i.bdate = @BDATE  
     AND i.cusip = @IDENTIFIER_VALUE  
END  
IF @IDENTIFIER_TYPE = 'SEDOL'  
BEGIN  
  INSERT #SECURITY  
        (mqa_id, ticker, cusip, sedol, isin, imnt_nm, country_cd)  
  SELECT i.mqa_id, i.ticker, i.cusip, i.sedol, i.isin, i.imnt_nm, i.country
    FROM instrument_characteristics i
   WHERE i.bdate = @BDATE  
     AND i.sedol = @IDENTIFIER_VALUE  
END  
  
UPDATE #SECURITY
   SET country_nm = d.decode
  FROM decode d
 WHERE d.item = 'COUNTRY'
   AND #SECURITY.country_cd = d.code

IF @DEBUG = 1  
BEGIN  
  SELECT '#SECURITY'  
  SELECT * FROM #SECURITY ORDER BY cusip, sedol  
END  
  
SELECT ticker  AS [Ticker],  
       cusip  AS [CUSIP],  
       sedol  AS [SEDOL],  
       isin  AS [ISIN],  
       imnt_nm  AS [Name],  
       country_nm AS [Country Name]  
  FROM #SECURITY 

CREATE TABLE #POSITION (  
  account_cd varchar(32) NULL,  
  cusip  varchar(32) NULL,  
  sedol  varchar(32) NULL,  
  units  float  NULL,  
  price  float  NULL,  
  mval  float  NULL,  
  weight float  NULL  
)  
  
IF @IDENTIFIER_TYPE IN ('TICKER', 'CUSIP')  
BEGIN  
  INSERT #POSITION (account_cd, cusip, sedol, units)  
  SELECT DISTINCT p.account_cd, p.cusip, p.sedol, p.units  
    FROM position p, account a  
   WHERE p.bdate = @BDATE  
     AND p.account_cd IN (SELECT DISTINCT n.account_cd  
                            FROM position n, #SECURITY s  
                           WHERE n.bdate = @BDATE  
                             AND n.cusip = s.cusip)  
     AND p.account_cd = a.account_cd  
     AND a.representative = 1  
  
  UPDATE #POSITION  
     SET price = i.price_close  
    FROM instrument_characteristics i  
   WHERE i.bdate = @BDATE  
     AND #POSITION.cusip = i.cusip  
END  
ELSE  
BEGIN  
  INSERT #POSITION (account_cd, cusip, sedol, units)  
  SELECT DISTINCT p.account_cd, p.cusip, p.sedol, p.units  
    FROM position p, account a  
   WHERE p.bdate = @BDATE  
     AND p.account_cd IN (SELECT DISTINCT n.account_cd  
         FROM position n, #SECURITY s  
                           WHERE n.bdate = @BDATE  
                             AND n.sedol = s.sedol)  
     AND p.account_cd = a.account_cd  
     AND a.representative = 1  
  
  UPDATE #POSITION  
     SET price = i.price_close  
    FROM instrument_characteristics i  
   WHERE i.bdate = @BDATE  
     AND #POSITION.sedol = i.sedol  
END  
  
UPDATE #POSITION  
   SET price = 1.0  
 WHERE cusip = '_USD'  
  
UPDATE #POSITION  
   SET mval = units * price  
  
UPDATE #POSITION  
   SET weight = mval / x.total_mval  
  FROM (SELECT account_cd, SUM(mval) AS total_mval FROM #POSITION GROUP BY account_cd) x  
 WHERE #POSITION.account_cd = x.account_cd  
  
IF @DEBUG = 1  
BEGIN  
  SELECT '#POSITION (1)'  
  SELECT * FROM #POSITION ORDER BY account_cd, cusip  
END  

CREATE TABLE #STRATEGY_ID ( strategy_id int NOT NULL )
EXEC access_strategy_get

IF @IDENTIFIER_TYPE IN ('TICKER', 'CUSIP')  
BEGIN  
  DELETE #POSITION  
    FROM #SECURITY s  
   WHERE #POSITION.cusip != s.cusip  
  
  INSERT #POSITION (account_cd, cusip, sedol, weight)  
  SELECT a.account_cd, y.cusip, y.sedol, 0.0  
    FROM #SECURITY y, scores s, account a  
   WHERE s.bdate = @BDATE  
     AND s.cusip = y.cusip  
     AND s.strategy_id = a.strategy_id 
     AND a.strategy_id IN (SELECT strategy_id FROM #STRATEGY_ID)
     AND a.representative = 1  
     AND a.account_cd NOT IN (SELECT account_cd FROM #POSITION)  
END  
ELSE  
BEGIN  
  DELETE #POSITION  
    FROM #SECURITY s  
   WHERE #POSITION.sedol != s.sedol  
  
  INSERT #POSITION (account_cd, cusip, sedol, weight)  
  SELECT a.account_cd, y.cusip, y.sedol, 0.0  
    FROM #SECURITY y, scores s, account a  
   WHERE s.bdate = @BDATE  
     AND s.sedol = y.sedol  
     AND s.strategy_id = a.strategy_id 
     AND a.strategy_id IN (SELECT strategy_id FROM #STRATEGY_ID)
     AND a.representative = 1  
     AND a.account_cd NOT IN (SELECT account_cd FROM #POSITION)  
END  
  
IF @DEBUG = 1  
BEGIN  
  SELECT '#POSITION (2)'  
  SELECT * FROM #POSITION ORDER BY account_cd, cusip  
END  
  
CREATE TABLE #RESULT (  
  strategy_id  int  NULL,  
  strategy_cd  varchar(16) NULL,  
  
  model_id  int  NULL,  
  model_cd  varchar(32) NULL,  
  
  account_cd  varchar(32) NULL,  
  representative bit  NULL,  
  
  bm_universe_id int  NULL,  
  bm_universe_cd varchar(32) NULL,  
  
  total_score  float  NULL,  
  
  account_wgt  float  NULL,  
  benchmark_wgt  float  NULL,  
  model_wgt  float  NULL,  
  
  acct_bm_wgt  float  NULL,  
  mpf_bm_wgt  float  NULL,  
  acct_mpf_wgt  float  NULL  
)  
  
INSERT #RESULT  
      (strategy_id, strategy_cd, account_cd, representative, bm_universe_id, bm_universe_cd, account_wgt)  
SELECT g.strategy_id, g.strategy_cd, a.account_cd, a.representative, d.universe_id, d.universe_cd, p.weight  
  FROM #POSITION p, account a, strategy g, universe_def d  
 WHERE p.account_cd = a.account_cd  
   AND a.strategy_id = g.strategy_id
   AND a.strategy_id IN (SELECT strategy_id FROM #STRATEGY_ID)
   AND a.bm_universe_id = d.universe_id  

DROP TABLE #STRATEGY_ID

IF @IDENTIFIER_TYPE IN ('TICKER', 'CUSIP')  
BEGIN  
  UPDATE #RESULT  
     SET benchmark_wgt = m.weight / 100.0  
    FROM #POSITION p, universe_makeup m  
   WHERE #RESULT.account_cd = p.account_cd  
     AND #RESULT.bm_universe_id = m.universe_id  
     AND m.universe_dt = @BDATE  
     AND m.cusip = p.cusip  
  
  UPDATE #RESULT  
     SET total_score = c.total_score  
    FROM scores c, #SECURITY s  
   WHERE c.bdate = @BDATE  
     AND #RESULT.strategy_id = c.strategy_id  
     AND s.cusip = c.cusip  
  
  UPDATE #RESULT  
     SET model_id = d2.universe_id,  
         model_cd = d2.universe_cd  
    FROM strategy g, universe_def d1, universe_def d2  
   WHERE #RESULT.strategy_id = g.strategy_id  
     AND g.universe_id = d1.universe_id  
     AND d2.universe_cd = d1.universe_cd + @MODEL_TYPE  
  
  UPDATE #RESULT  
     SET model_wgt = m.weight / 100.0  
    FROM #POSITION p, universe_makeup m  
   WHERE #RESULT.account_cd = p.account_cd  
     AND #RESULT.model_id = m.universe_id  
     AND m.universe_dt = @BDATE  
     AND m.cusip = p.cusip  
END  
ELSE  
BEGIN  
  UPDATE #RESULT  
     SET benchmark_wgt = m.weight / 100.0  
    FROM #POSITION p, universe_makeup m  
   WHERE #RESULT.account_cd = p.account_cd  
     AND #RESULT.bm_universe_id = m.universe_id  
     AND m.universe_dt = @BDATE  
     AND m.sedol = p.sedol  
  
  UPDATE #RESULT  
     SET total_score = c.total_score  
    FROM scores c, #SECURITY s  
   WHERE c.bdate = @BDATE  
     AND #RESULT.strategy_id = c.strategy_id  
     AND s.sedol = c.sedol  
  
  UPDATE #RESULT  
     SET model_id = d2.universe_id,  
         model_cd = d2.universe_cd  
    FROM strategy g, universe_def d1, universe_def d2  
   WHERE #RESULT.strategy_id = g.strategy_id  
     AND g.universe_id = d1.universe_id  
     AND d2.universe_cd = d1.universe_cd + @MODEL_TYPE  
  
  UPDATE #RESULT  
     SET model_wgt = m.weight / 100.0  
    FROM #POSITION p, universe_makeup m  
   WHERE #RESULT.account_cd = p.account_cd  
     AND #RESULT.model_id = m.universe_id  
     AND m.universe_dt = @BDATE  
     AND m.sedol = p.sedol  
END  
  
IF @DEBUG = 1  
BEGIN  
  SELECT '#RESULT (1)'  
  SELECT * FROM #RESULT ORDER BY strategy_cd, account_cd, bm_universe_cd  
END  
  
DELETE #RESULT  
 WHERE total_score IS NULL  
  
UPDATE #RESULT  
   SET benchmark_wgt = 0.0  
 WHERE benchmark_wgt IS NULL  
  
UPDATE #RESULT  
   SET model_wgt = 0.0  
 WHERE model_wgt IS NULL  

UPDATE #RESULT
   SET model_wgt = NULL
  FROM decode d
 WHERE d.item = 'MODEL WEIGHT NULL'
   AND #RESULT.strategy_id = d.code

UPDATE #RESULT  
   SET acct_bm_wgt = account_wgt - benchmark_wgt,  
       mpf_bm_wgt = model_wgt - benchmark_wgt,  
       acct_mpf_wgt = account_wgt - model_wgt  
  
IF @DEBUG = 1  
BEGIN  
  SELECT '#RESULT (2)'  
  SELECT * FROM #RESULT ORDER BY strategy_cd, account_cd, bm_universe_cd  
END  
  
SELECT strategy_cd		AS [Strategy],
       account_cd		AS [Portfolio],
       bm_universe_cd	AS [Benchmark],
       ROUND(total_score,1) AS [Total Score],
       account_wgt		AS [Account Wgt],
       benchmark_wgt	AS [Benchmark Wgt],
       model_wgt		AS [Model Wgt],
       acct_bm_wgt		AS [Acct-Bmk Wgt],
       mpf_bm_wgt		AS [Model-Bmk Wgt],
       acct_mpf_wgt		AS [Acct-Model Wgt]
  FROM #RESULT
 ORDER BY strategy_cd, bm_universe_cd, representative DESC, account_cd
  
RETURN 0
go
IF OBJECT_ID('dbo.rpt_strategy_view_current') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_strategy_view_current >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_strategy_view_current >>>'
go
