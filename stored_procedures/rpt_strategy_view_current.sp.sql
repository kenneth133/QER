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
CREATE PROCEDURE dbo.rpt_strategy_view_current
@BDATE datetime,
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
IF @IDENTIFIER_TYPE NOT IN ('TICKER', 'CUSIP', 'SEDOL', 'ISIN')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @IDENTIFIER_TYPE PARAMETER' RETURN -1 END
IF @IDENTIFIER_VALUE IS NULL
  BEGIN SELECT 'ERROR: @IDENTIFIER_VALUE IS A REQUIRED PARAMETER' RETURN -1 END

IF @MODEL_TYPE = 'EQUAL'
  BEGIN SELECT @MODEL_TYPE = '_MPF_EQL' END
ELSE IF @MODEL_TYPE = 'CAP'
  BEGIN SELECT @MODEL_TYPE = '_MPF_CAP' END

CREATE TABLE #SECURITY (
  bdate			datetime		NULL,
  security_id	int				NULL,
  ticker		varchar(32)		NULL,
  cusip			varchar(32)		NULL,
  sedol			varchar(32)		NULL,
  isin			varchar(32)		NULL,
  imnt_nm		varchar(100)	NULL,
  currency_cd	varchar(3)		NULL,
  exchange_nm	varchar(60)		NULL,
  country_cd	varchar(50)		NULL,
  country_nm	varchar(128)	NULL
)

IF @IDENTIFIER_TYPE = 'TICKER'
  BEGIN INSERT #SECURITY (bdate, ticker) VALUES (@BDATE, @IDENTIFIER_VALUE) END
ELSE IF @IDENTIFIER_TYPE = 'CUSIP'
BEGIN
  INSERT #SECURITY (bdate, cusip) VALUES (@BDATE, @IDENTIFIER_VALUE)
  UPDATE #SECURITY SET cusip = equity_common.dbo.fnCusipIncludeCheckDigit(cusip)
END
ELSE IF @IDENTIFIER_TYPE = 'SEDOL'
BEGIN
  INSERT #SECURITY (bdate, sedol) VALUES (@BDATE, @IDENTIFIER_VALUE)
  UPDATE #SECURITY SET sedol = equity_common.dbo.fnSedolIncludeCheckDigit(sedol)
END
ELSE IF @IDENTIFIER_TYPE = 'ISIN'
  BEGIN INSERT #SECURITY (bdate, isin) VALUES (@BDATE, @IDENTIFIER_VALUE) END

EXEC security_id_update @TABLE_NAME='#SECURITY'

IF @DEBUG = 1
BEGIN
  SELECT '#SECURITY (1)'
  SELECT * FROM #SECURITY
END

IF NOT EXISTS (SELECT 1 FROM #SECURITY WHERE security_id IS NOT NULL)
  BEGIN RETURN 0 END

DECLARE @SECURITY_ID int
SELECT @SECURITY_ID = security_id FROM #SECURITY

UPDATE #SECURITY
   SET ticker = y.ticker,
       cusip = y.cusip,
       sedol = y.sedol,
       isin = y.isin,
       imnt_nm = y.security_name,
       country_cd = ISNULL(y.domicile_iso_cd, y.issue_country_cd)
  FROM equity_common..security y
 WHERE y.security_id = @SECURITY_ID

UPDATE #SECURITY
   SET country_nm = UPPER(c.country_name)
  FROM equity_common..country c
 WHERE #SECURITY.country_cd = c.country_cd

IF @DEBUG = 1
BEGIN
  SELECT '#SECURITY (2)'
  SELECT * FROM #SECURITY
END

SELECT ticker	AS [Ticker],
       cusip	AS [CUSIP],
       sedol	AS [SEDOL],
       isin		AS [ISIN],
       imnt_nm	AS [Name],
       country_nm AS [Country Name]
  FROM #SECURITY

DROP TABLE #SECURITY

CREATE TABLE #STRATEGY_ID ( strategy_id int NOT NULL )
EXEC access_strategy_get

IF @DEBUG = 1
BEGIN
  SELECT '#STRATEGY_ID'
  SELECT * FROM #STRATEGY_ID
END

CREATE TABLE #SCORE (
  strategy_id	int		NULL,
  total_score	float	NULL
)

INSERT #SCORE
SELECT strategy_id, total_score
  FROM scores
 WHERE bdate = @BDATE
   AND strategy_id IN (SELECT strategy_id FROM #STRATEGY_ID)
   AND security_id = @SECURITY_ID
   AND total_score IS NOT NULL

IF @DEBUG = 1
BEGIN
  SELECT '#SCORE'
  SELECT * FROM #SCORE
END

DROP TABLE #STRATEGY_ID

CREATE TABLE #POSITION (
  account_cd	varchar(50) NULL,
  security_id	int			NULL,
  units			float		NULL,
  price			float		NULL,
  mval			float		NULL,
  weight		float		NULL
)

INSERT #POSITION (account_cd, security_id, units, price, weight)
SELECT p.acct_cd, p.security_id, SUM(ISNULL(p.quantity,0.0)), 0.0, 0.0
  FROM equity_common..position p
 WHERE p.reference_date = @BDATE
   AND p.reference_date = p.effective_date
   AND p.acct_cd IN (SELECT DISTINCT n.acct_cd
                       FROM equity_common..position n
                      WHERE n.reference_date = @BDATE
                        AND n.reference_date = n.effective_date
                        AND n.acct_cd IN (SELECT a1.acct_cd FROM equity_common..account a1, account b1
                                           WHERE b1.strategy_id IN (SELECT strategy_id FROM #SCORE)
                                             AND a1.parent = b1.account_cd AND b1.representative = 1
                                          UNION
                                          SELECT a2.acct_cd FROM equity_common..account a2, account b2
                                           WHERE b2.strategy_id IN (SELECT strategy_id FROM #SCORE)
                                             AND a2.acct_cd = b2.account_cd AND b2.representative = 1)
                        AND n.security_id = @SECURITY_ID)
 GROUP BY p.acct_cd, p.security_id

UPDATE #POSITION
   SET price = ISNULL(p.price_close_usd,0.0)
  FROM equity_common..market_price p
 WHERE p.reference_date = @BDATE
   AND #POSITION.security_id = p.security_id

UPDATE #POSITION
   SET mval = units * price

IF @DEBUG = 1
BEGIN
  SELECT '#POSITION (1)'
  SELECT * FROM #POSITION ORDER BY account_cd, security_id
END

UPDATE #POSITION
   SET weight = mval / x.total_mval
  FROM (SELECT account_cd, SUM(mval) AS total_mval FROM #POSITION GROUP BY account_cd) x
 WHERE #POSITION.account_cd = x.account_cd
   AND x.total_mval != 0.0

IF @DEBUG = 1
BEGIN
  SELECT '#POSITION (2)'
  SELECT * FROM #POSITION ORDER BY account_cd, security_id
END

CREATE TABLE #RESULT (
  strategy_id		int			NULL,  
  strategy_cd		varchar(16)	NULL,  
  model_id			int			NULL,
  model_cd			varchar(32)	NULL,

  account_cd		varchar(32)	NULL,
  benchmark_cd		varchar(32)	NULL,
  total_score		float		NULL,

  account_wgt		float		NULL,
  benchmark_wgt		float		NULL,
  model_wgt			float		NULL,

  acct_bm_wgt		float		NULL,
  mpf_bm_wgt		float		NULL,
  acct_mpf_wgt		float		NULL
)

INSERT #RESULT (strategy_id, account_cd, benchmark_cd, total_score, account_wgt, benchmark_wgt, model_wgt)
SELECT s.strategy_id, a.account_cd, a.benchmark_cd, s.total_score, 0.0, 0.0, 0.0
  FROM #SCORE s, account a
 WHERE s.strategy_id = a.strategy_id
   AND a.representative = 1

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (1)'
  SELECT * FROM #RESULT ORDER BY strategy_cd, account_cd, benchmark_cd
END

UPDATE #RESULT
   SET account_wgt = p.weight
  FROM #POSITION p
 WHERE #RESULT.account_cd = p.account_cd
   AND p.security_id = @SECURITY_ID

DROP TABLE #POSITION
DROP TABLE #SCORE

UPDATE #RESULT
   SET benchmark_wgt = w.weight
  FROM equity_common..benchmark_weight w
 WHERE #RESULT.benchmark_cd IN (SELECT benchmark_cd FROM benchmark)
   AND #RESULT.benchmark_cd = w.acct_cd
   AND w.reference_date = @BDATE
   AND w.reference_date = w.effective_date
   AND w.security_id = @SECURITY_ID

UPDATE #RESULT
   SET benchmark_wgt = m.weight / 100.0
  FROM universe_def d, universe_makeup m
 WHERE #RESULT.benchmark_cd = d.universe_cd
   AND d.universe_id = m.universe_id
   AND m.universe_dt = @BDATE
   AND m.security_id = @SECURITY_ID
   AND #RESULT.benchmark_wgt IS NULL

UPDATE #RESULT
   SET strategy_cd = g.strategy_cd,
       model_id = d2.universe_id,
       model_cd = d2.universe_cd
  FROM strategy g, universe_def d1, universe_def d2
 WHERE #RESULT.strategy_id = g.strategy_id
   AND g.universe_id = d1.universe_id
   AND d2.universe_cd = d1.universe_cd + @MODEL_TYPE

UPDATE #RESULT
   SET model_wgt = m.weight / 100.0
  FROM universe_makeup m
 WHERE #RESULT.model_id = m.universe_id
   AND m.universe_dt = @BDATE
   AND m.security_id = @SECURITY_ID

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
  SELECT * FROM #RESULT ORDER BY strategy_cd, account_cd, benchmark_cd
END

SELECT strategy_cd		AS [Strategy],
       account_cd		AS [Portfolio],
       benchmark_cd		AS [Benchmark],
       ROUND(total_score,1) AS [Total Score],
       account_wgt		AS [Account Wgt],
       benchmark_wgt	AS [Benchmark Wgt],
       model_wgt		AS [Model Wgt],
       acct_bm_wgt		AS [Acct-Bmk Wgt],
       mpf_bm_wgt		AS [Model-Bmk Wgt],
       acct_mpf_wgt		AS [Acct-Model Wgt]
  FROM #RESULT
 ORDER BY strategy_cd, benchmark_cd, account_cd

DROP TABLE #RESULT

RETURN 0
go
IF OBJECT_ID('dbo.rpt_strategy_view_current') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_strategy_view_current >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_strategy_view_current >>>'
go
