use QER
go
IF OBJECT_ID('dbo.rpt_segment_summary') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rpt_segment_summary
    IF OBJECT_ID('dbo.rpt_segment_summary') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rpt_segment_summary >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rpt_segment_summary >>>'
END
go
CREATE PROCEDURE dbo.rpt_segment_summary @STRATEGY_ID int,
                                         @BDATE datetime,
                                         @ACCOUNT_CD varchar(32),
                                         @WEIGHT varchar(16),
                                         @SEGMENT_BY varchar(32),
                                         @DEBUG bit = NULL
AS
/* PORTFOLIO - BY SEGMENT */

IF @STRATEGY_ID IS NULL
  BEGIN SELECT 'ERROR: @STRATEGY_ID IS A REQUIRED PARAMETER' RETURN -1 END
IF @BDATE IS NULL
  BEGIN SELECT 'ERROR: @BDATE IS A REQUIRED PARAMETER' RETURN -1 END
IF @ACCOUNT_CD IS NULL
  BEGIN SELECT 'ERROR: @ACCOUNT_CD IS A REQUIRED PARAMETER' RETURN -1 END
IF @WEIGHT IS NULL
  BEGIN SELECT 'ERROR: @WEIGHT IS A REQUIRED PARAMETER' RETURN -1 END
IF @WEIGHT NOT IN ('EQUAL', 'CAP')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @WEIGHT PARAMETER' RETURN -1 END
IF @SEGMENT_BY IS NULL
  BEGIN SELECT 'ERROR: @SEGMENT_BY IS A REQUIRED PARAMETER' RETURN -1 END
IF @SEGMENT_BY NOT IN ('SECTOR','SEGMENT','COUNTRY')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @SEGMENT_BY PARAMETER' RETURN -1 END

DECLARE @SECTOR_MODEL_ID int,
        @BENCHMARK_CD varchar(50),
        @MODEL_ID int

SELECT @SECTOR_MODEL_ID = m.sector_model_id
  FROM strategy g, factor_model m
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = m.factor_model_id

SELECT @BENCHMARK_CD = benchmark_cd
  FROM account
 WHERE strategy_id = @STRATEGY_ID
   AND account_cd = @ACCOUNT_CD

EXEC sector_model_security_populate @BDATE=@BDATE, @UNIVERSE_DT=@BDATE, @UNIVERSE_CD=@BENCHMARK_CD, @SECTOR_MODEL_ID=@SECTOR_MODEL_ID, @DEBUG=@DEBUG

CREATE TABLE #RESULT (
  security_id	int			NULL,
  country_cd	varchar(4)	NULL,
  sector_id		int			NULL,
  segment_id	int			NULL,

  units			float		NULL,
  price			float		NULL,
  mval			float		NULL,

  account_wgt	float		NULL,
  benchmark_wgt	float		NULL,
  model_wgt		float		NULL
)

IF @WEIGHT = 'EQUAL'
BEGIN
  SELECT @MODEL_ID = d2.universe_id
    FROM strategy g, universe_def d1, universe_def d2
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.universe_id = d1.universe_id
     AND d2.universe_cd = d1.universe_cd + '_MPF_EQL'

  IF EXISTS (SELECT 1 FROM account a, benchmark b
              WHERE a.strategy_id = @STRATEGY_ID
                AND a.account_cd = @ACCOUNT_CD
                AND a.benchmark_cd = b.benchmark_cd)
  BEGIN
    INSERT #RESULT (security_id)
    SELECT security_id
      FROM equity_common..benchmark_weight
     WHERE reference_date = @BDATE
       AND reference_date = effective_date
       AND acct_cd = @BENCHMARK_CD
  END
  ELSE
  BEGIN
    INSERT #RESULT (security_id)
    SELECT p.security_id
      FROM universe_def d, universe_makeup p
     WHERE d.universe_cd = @BENCHMARK_CD
       AND d.universe_id = p.universe_id
       AND p.universe_dt = @BDATE
  END

  UPDATE #RESULT
     SET benchmark_wgt = 1.0 / (SELECT COUNT(*) FROM #RESULT)
END
ELSE IF @WEIGHT = 'CAP'
BEGIN
  SELECT @MODEL_ID = d2.universe_id
    FROM strategy g, universe_def d1, universe_def d2
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.universe_id = d1.universe_id
     AND d2.universe_cd = d1.universe_cd + '_MPF_CAP'

  IF EXISTS (SELECT 1 FROM account a, benchmark b
              WHERE a.strategy_id = @STRATEGY_ID
                AND a.account_cd = @ACCOUNT_CD
                AND a.benchmark_cd = b.benchmark_cd)
  BEGIN
    INSERT #RESULT (security_id, benchmark_wgt)
    SELECT security_id, weight
      FROM equity_common..benchmark_weight
     WHERE reference_date = @BDATE
       AND reference_date = effective_date
       AND acct_cd = @BENCHMARK_CD
  END
  ELSE
  BEGIN
    INSERT #RESULT (security_id, benchmark_wgt)
    SELECT p.security_id, p.weight / 100.0
      FROM universe_def d, universe_makeup p
     WHERE d.universe_cd = @BENCHMARK_CD
       AND d.universe_id = p.universe_id
       AND p.universe_dt = @BDATE
  END
END

INSERT #RESULT (security_id)
SELECT security_id
  FROM universe_makeup
 WHERE universe_dt = @BDATE
   AND universe_id = @MODEL_ID
   AND security_id IS NOT NULL
   AND security_id NOT IN (SELECT security_id FROM #RESULT WHERE security_id IS NOT NULL)

INSERT #RESULT (security_id)
SELECT DISTINCT security_id
  FROM equity_common..position
 WHERE reference_date = @BDATE
   AND reference_date = effective_date
   AND acct_cd IN (SELECT DISTINCT acct_cd FROM equity_common..account WHERE parent = @ACCOUNT_CD OR acct_cd = @ACCOUNT_CD)
   AND security_id IS NOT NULL
   AND security_id NOT IN (SELECT security_id FROM #RESULT WHERE security_id IS NOT NULL)

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT: AFTER INITIAL INSERTS'
  SELECT * FROM #RESULT ORDER BY security_id
END

UPDATE #RESULT
   SET benchmark_wgt = 0.0
 WHERE benchmark_wgt IS NULL

UPDATE #RESULT
   SET model_wgt = p.weight / 100.0
  FROM universe_makeup p
 WHERE p.universe_dt = @BDATE
   AND p.universe_id = @MODEL_ID
   and #RESULT.security_id = p.security_id

UPDATE #RESULT
   SET model_wgt = 0.0
 WHERE model_wgt IS NULL

IF EXISTS (SELECT * FROM decode WHERE item='MODEL WEIGHT NULL' AND code=@STRATEGY_ID)
  BEGIN UPDATE #RESULT SET model_wgt = NULL END

UPDATE #RESULT
   SET units = x.quantity
  FROM (SELECT security_id, SUM(ISNULL(quantity,0.0)) AS [quantity]
          FROM equity_common..position
         WHERE reference_date = @BDATE
           AND reference_date = effective_date
           AND acct_cd IN (SELECT DISTINCT acct_cd FROM equity_common..account WHERE parent = @ACCOUNT_CD OR acct_cd = @ACCOUNT_CD)
         GROUP BY security_id) x
 WHERE #RESULT.security_id = x.security_id

UPDATE #RESULT
   SET country_cd = y.issue_country_cd
  FROM equity_common..security y
 WHERE #RESULT.security_id = y.security_id

UPDATE #RESULT
   SET price = p.price_close_usd
  FROM equity_common..market_price p
 WHERE #RESULT.security_id = p.security_id
   AND p.reference_date = @BDATE

UPDATE #RESULT
   SET mval = units * price

DECLARE @ACCOUNT_MVAL float

SELECT @ACCOUNT_MVAL = SUM(mval)
  FROM #RESULT

IF @ACCOUNT_MVAL != 0.0
BEGIN
  UPDATE #RESULT
     SET account_wgt = mval / @ACCOUNT_MVAL
END
ELSE
  BEGIN UPDATE #RESULT SET account_wgt = 0.0 END

UPDATE #RESULT
   SET account_wgt = 0.0
 WHERE account_wgt IS NULL

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT: AFTER UPDATES'
  SELECT * FROM #RESULT ORDER BY security_id
END

CREATE TABLE #RESULT2 (
  country_cd		varchar(4)		NULL,
  country_nm		varchar(128)	NULL,
  sector_id			int				NULL,
  sector_num		int				NULL,
  sector_nm			varchar(64)		NULL,
  segment_id		int				NULL,
  segment_num		int				NULL,
  segment_nm		varchar(128)	NULL,
  account_wgt		float			NULL,
  benchmark_wgt		float			NULL,
  model_wgt			float			NULL,
  acct_bmk_wgt		float			NULL,
  model_bmk_wgt		float			NULL,
  acct_model_wgt	float			NULL
)

IF @SEGMENT_BY IN ('SECTOR','SEGMENT')
BEGIN
  DECLARE @DUMMY_UNIVERSE_ID int
  SELECT @DUMMY_UNIVERSE_ID = universe_id FROM universe_def WHERE universe_cd = 'DUMMY'

  INSERT universe_makeup (universe_dt, universe_id, security_id)
  SELECT @BDATE, @DUMMY_UNIVERSE_ID, r.security_id
    FROM #RESULT r
   WHERE NOT EXISTS (SELECT 1 FROM sector_model_security ss
                      WHERE ss.bdate = @BDATE
                        AND ss.sector_model_id = @SECTOR_MODEL_ID
                        AND ss.security_id = r.security_id)

  IF @DEBUG = 1
  BEGIN
    SELECT 'DUMMY UNIVERSE TO BE CLASSIFIED'
    SELECT * FROM universe_makeup
     WHERE universe_id = @DUMMY_UNIVERSE_ID
     ORDER BY universe_dt, security_id
  END

  EXEC sector_model_security_populate @BDATE=@BDATE, @SECTOR_MODEL_ID=@SECTOR_MODEL_ID, @UNIVERSE_DT=@BDATE, @UNIVERSE_ID=@DUMMY_UNIVERSE_ID

  DELETE universe_makeup
   WHERE universe_id = @DUMMY_UNIVERSE_ID

  UPDATE #RESULT
     SET sector_id = ss.sector_id,
         segment_id = ss.segment_id
    FROM sector_model_security ss
   WHERE ss.bdate = @BDATE
     AND ss.sector_model_id = @SECTOR_MODEL_ID
     AND #RESULT.security_id = ss.security_id

  UPDATE #RESULT
     SET sector_id = -1,
         segment_id = -1
   WHERE security_id IN (SELECT security_id FROM equity_common..security WHERE cusip = '_USD')

  UPDATE #RESULT
     SET sector_id = -2
   WHERE sector_id IS NULL

  UPDATE #RESULT
     SET segment_id = -2
   WHERE segment_id IS NULL

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT: AFTER SECTOR AND SEGMENT UPDATES'
    SELECT * FROM #RESULT ORDER BY sector_id, segment_id
  END

  IF @SEGMENT_BY = 'SECTOR'
  BEGIN
    INSERT #RESULT2
          (sector_id, account_wgt, benchmark_wgt, model_wgt)
    SELECT sector_id, SUM(account_wgt), SUM(benchmark_wgt), SUM(model_wgt)
      FROM #RESULT
     GROUP BY sector_id
  END

  IF @SEGMENT_BY = 'SEGMENT'
  BEGIN
    INSERT #RESULT2
          (sector_id, segment_id, account_wgt, benchmark_wgt, model_wgt)
    SELECT sector_id, segment_id, SUM(account_wgt), SUM(benchmark_wgt), SUM(model_wgt)
      FROM #RESULT
     GROUP BY sector_id, segment_id
  END

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT2: AFTER INITIAL INSERT'
    SELECT * FROM #RESULT2 ORDER BY sector_id, segment_id
  END

  UPDATE #RESULT2
     SET acct_bmk_wgt = account_wgt - benchmark_wgt,
         model_bmk_wgt = model_wgt - benchmark_wgt,
         acct_model_wgt = account_wgt - model_wgt

  UPDATE #RESULT2
     SET sector_num = d.sector_num,
         sector_nm = d.sector_nm
    FROM sector_def d
   WHERE #RESULT2.sector_id = d.sector_id

  UPDATE #RESULT2
     SET segment_num = d.segment_num,
         segment_nm = d.segment_nm
    FROM segment_def d
   WHERE #RESULT2.segment_id = d.segment_id

  UPDATE #RESULT2
     SET sector_num = 9999,
         sector_nm = 'CASH',
         segment_num = 9999,
         segment_nm = 'CASH'
   WHERE sector_id = -1

  UPDATE #RESULT2
     SET sector_num = 9998,
         sector_nm = 'UNKNOWN'
   WHERE sector_id = -2

  UPDATE #RESULT2
     SET segment_num = 9998,
         segment_nm = 'UNKNOWN'
   WHERE segment_id = -2

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT2: AFTER UPDATES'
    SELECT * FROM #RESULT2 ORDER BY sector_id, segment_id
  END

  IF @SEGMENT_BY = 'SECTOR'
  BEGIN
    SELECT sector_nm		AS [Sector Name],
           account_wgt		AS [Account],
           benchmark_wgt	AS [Benchmark],
           model_wgt		AS [Model],
           acct_bmk_wgt		AS [Acct-Bmk],
           model_bmk_wgt	AS [Model-Bmk],
           acct_model_wgt	AS [Acct-Model]
      FROM #RESULT2
     ORDER BY sector_num
  END

  IF @SEGMENT_BY = 'SEGMENT'
  BEGIN
    SELECT sector_nm		AS [Sector Name],
           segment_nm		AS [Segment Name],
           account_wgt		AS [Account],
           benchmark_wgt	AS [Benchmark],
           model_wgt		AS [Model],
           acct_bmk_wgt		AS [Acct-Bmk],
           model_bmk_wgt	AS [Model-Bmk],
           acct_model_wgt	AS [Acct-Model]
      FROM #RESULT2
     ORDER BY sector_num, segment_num
  END
END

IF @SEGMENT_BY IN ('COUNTRY')
BEGIN
  INSERT #RESULT2
        (country_cd, account_wgt, benchmark_wgt, model_wgt)
  SELECT country_cd, SUM(account_wgt), SUM(benchmark_wgt), SUM(model_wgt)
    FROM #RESULT
   GROUP BY country_cd

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT2: AFTER INITIAL INSERT'
    SELECT * FROM #RESULT2 ORDER BY country_cd
  END

  UPDATE #RESULT2
     SET acct_bmk_wgt = account_wgt - benchmark_wgt,
         model_bmk_wgt = model_wgt - benchmark_wgt,
         acct_model_wgt = account_wgt - model_wgt

  UPDATE #RESULT2
     SET country_nm = UPPER(c.country_name)
    FROM equity_common..country c
   WHERE #RESULT2.country_cd = c.country_cd

  UPDATE #RESULT2
     SET country_nm = 'UNKNOWN'
   WHERE country_nm IS NULL

  IF @DEBUG = 1
  BEGIN
    SELECT '#RESULT2: AFTER UPDATES'
    SELECT * FROM #RESULT2 ORDER BY country_cd
  END

  SELECT country_nm		AS [Country Name],
         account_wgt	AS [Account],
         benchmark_wgt	AS [Benchmark],
         model_wgt		AS [Model],
         acct_bmk_wgt	AS [Acct-Bmk],
         model_bmk_wgt	AS [Model-Bmk],
         acct_model_wgt	AS [Acct-Model]
    FROM #RESULT2
   ORDER BY country_nm
END

DROP TABLE #RESULT2
DROP TABLE #RESULT

RETURN 0
go
IF OBJECT_ID('dbo.rpt_segment_summary') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_segment_summary >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_segment_summary >>>'
go
