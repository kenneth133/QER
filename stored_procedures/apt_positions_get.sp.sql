use QER
go

IF OBJECT_ID('dbo.apt_positions_get') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.apt_positions_get
    IF OBJECT_ID('dbo.apt_positions_get') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.apt_positions_get >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.apt_positions_get >>>'
END
go

CREATE PROCEDURE dbo.apt_positions_get
@APT_GROUP varchar(64),
@BDATE datetime = NULL,
@DEBUG bit = NULL
AS

IF @APT_GROUP IS NULL
  BEGIN SELECT 'ERROR: @APT_GROUP IS A REQUIRED PARAMETER' RETURN -1 END

IF @BDATE IS NULL
BEGIN
  SELECT @BDATE = MAX(reference_date)
    FROM equity_common..position
   WHERE acct_cd IN (SELECT code FROM decode WHERE item = @APT_GROUP)
END

CREATE TABLE #ACCOUNT (
  parent		varchar(50)	NOT NULL,
  acct_cd		varchar(50)	NULL,
  benchmark_cd	varchar(32)	NULL,
  category		varchar(64)	NULL
)

INSERT #ACCOUNT
SELECT a.acct_cd, a.acct_cd, a.account_benchmark, d.decode
  FROM decode d, equity_common..account a
 WHERE d.item = @APT_GROUP
   AND d.code = a.acct_cd
   AND a.account_type_cd != 'G'

INSERT #ACCOUNT
SELECT a1.acct_cd, a2.acct_cd, a1.account_benchmark, d.decode
  FROM decode d, equity_common..account a1, equity_common..account a2
 WHERE d.item = @APT_GROUP
   AND d.code = a1.acct_cd
   AND a1.account_type_cd = 'G'
   AND a1.acct_cd = a2.parent

UPDATE #ACCOUNT
   SET benchmark_cd = 'CASH'
 WHERE benchmark_cd = '90TBILLS'

IF @DEBUG = 1
BEGIN
  SELECT '#ACCOUNT (1)'
  SELECT * FROM #ACCOUNT ORDER BY parent, acct_cd
END

CREATE TABLE #ACCOUNT_POSITION (
  acct_cd		varchar(50)	NOT NULL,
  security_id	int			NOT NULL,
  ls_flag		bit			NOT NULL,
  quantity		float		NULL
)

INSERT #ACCOUNT_POSITION
SELECT a.parent, p.security_id, 1, SUM(p.quantity)
  FROM #ACCOUNT a, equity_common..position p
 WHERE p.reference_date = @BDATE
   AND p.reference_date = p.effective_date
   AND p.acct_cd = a.acct_cd
   AND p.security_id IS NOT NULL
 GROUP BY a.parent, p.security_id

DELETE #ACCOUNT_POSITION
 WHERE quantity = 0.0

UPDATE #ACCOUNT_POSITION
   SET ls_flag = 0
 WHERE quantity < 0.0

IF @DEBUG = 1
BEGIN
  SELECT '#ACCOUNT_POSITION'
  SELECT * FROM #ACCOUNT_POSITION
END

INSERT #ACCOUNT
SELECT DISTINCT parent, NULL, benchmark_cd, category
  FROM #ACCOUNT
 WHERE parent != acct_cd

DELETE #ACCOUNT
 WHERE parent != acct_cd
   AND acct_cd IS NOT NULL

IF @DEBUG = 1
BEGIN
  SELECT '#ACCOUNT (2)'
  SELECT * FROM #ACCOUNT
END

CREATE TABLE #BENCHMARK_POSITION (
  benchmark_cd	varchar(50)		NOT NULL,
  security_id	int				NULL,
  weight		float			NULL
)

INSERT #BENCHMARK_POSITION
SELECT acct_cd, security_id, weight
  FROM equity_common..benchmark_weight
 WHERE reference_date = @BDATE
   AND reference_date = effective_date
   AND acct_cd IN (SELECT DISTINCT benchmark_cd FROM #ACCOUNT)
   AND security_id IS NOT NULL

IF NOT EXISTS (SELECT * FROM #BENCHMARK_POSITION WHERE benchmark_cd = 'CASH')
BEGIN
  INSERT #BENCHMARK_POSITION
  SELECT 'CASH', security_id, 1.0
    FROM equity_common..security
   WHERE (ticker = '_USD' OR cusip = '_USD')
     AND security_type_class_cd = 'CURR'
END

IF @DEBUG = 1
BEGIN
  SELECT '#BENCHMARK_POSITION'
  SELECT * FROM #BENCHMARK_POSITION
END

CREATE TABLE #RESULT (
  category			varchar(64)		NULL,
  acct_cd			varchar(50)		NULL,
  security_id		int				NULL,
  ls_flag			bit				NULL,
  quantity			float			NULL,
  price_close		float			NULL,
  benchmark_cd		varchar(50)		NULL,
  benchmark_wgt		float			NULL
)

INSERT #RESULT
SELECT a.category, a.parent, ap.security_id, ap.ls_flag, ap.quantity, NULL, a.benchmark_cd, NULL
  FROM #ACCOUNT a, #ACCOUNT_POSITION ap
 WHERE a.parent = ap.acct_cd

INSERT #RESULT
SELECT a.category, a.parent, bp.security_id, 1, 0.0, NULL, a.benchmark_cd, NULL
  FROM #ACCOUNT a, #BENCHMARK_POSITION bp
 WHERE a.benchmark_cd = bp.benchmark_cd
   AND NOT EXISTS (SELECT 1 FROM #ACCOUNT_POSITION ap
                    WHERE a.parent = ap.acct_cd
                      AND ap.security_id = bp.security_id)

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT: INITIAL INSERT'
  SELECT * FROM #RESULT ORDER BY acct_cd, security_id
END

UPDATE #RESULT
   SET price_close = p.price_close_usd
  FROM equity_common..market_price p
 WHERE p.reference_date = @BDATE
   AND #RESULT.security_id = p.security_id

UPDATE #RESULT
   SET benchmark_wgt = bp.weight
  FROM #BENCHMARK_POSITION bp
 WHERE #RESULT.benchmark_cd = bp.benchmark_cd
   AND #RESULT.security_id = bp.security_id

UPDATE #RESULT
   SET benchmark_wgt = 0.0
 WHERE benchmark_wgt IS NULL

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT: FINAL STATE'
  SELECT * FROM #RESULT ORDER BY acct_cd, security_id
END

SELECT r.category		AS [Fund Category],
       a1.account_name	AS [Portfolio],
       CASE y.cusip WHEN '_USD' THEN '$CASH' ELSE y.cusip END AS [Cusip9],
       CASE y.ticker WHEN '_USD' THEN '$CASH' ELSE y.ticker END AS [Ticker],
       r.ls_flag		AS [L/S Flag],
       r.quantity		AS [Shares],
       1				AS [Unit Size],
       r.price_close	AS [Price],
       1				AS [fxRate],
       'USD'			AS [Currency],
       'USD'			AS [Investor Currency],
       a1.acct_cd		AS [Account Code],
       a2.account_name	AS [Benchmark Name],
       r.benchmark_wgt	AS [Benchmark Weight]
  FROM #RESULT r,
       equity_common..account a1,
       equity_common..account a2,
       equity_common..security y
 WHERE r.acct_cd = a1.acct_cd
   AND r.benchmark_cd = a2.acct_cd
   AND r.security_id = y.security_id
 ORDER BY r.acct_cd, y.cusip

DROP TABLE #RESULT
DROP TABLE #BENCHMARK_POSITION
DROP TABLE #ACCOUNT_POSITION
DROP TABLE #ACCOUNT

RETURN 0
go

IF OBJECT_ID('dbo.apt_positions_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.apt_positions_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.apt_positions_get >>>'
go
