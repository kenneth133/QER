use QER
go
IF OBJECT_ID('dbo.rpt_model_summary') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rpt_model_summary
    IF OBJECT_ID('dbo.rpt_model_summary') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rpt_model_summary >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rpt_model_summary >>>'
END
go
CREATE PROCEDURE dbo.rpt_model_summary @STRATEGY_ID int,
                                       @BDATE datetime,
                                       @ACCOUNT_CD varchar(32),
                                       @COUNTRY_CD varchar(4) = NULL,
                                       @SECTOR_ID int = NULL,
                                       @SEGMENT_ID int = NULL,
                                       @DEBUG bit = NULL
AS
/* MODEL - SUMMARY */

IF @STRATEGY_ID IS NULL
  BEGIN SELECT 'ERROR: @STRATEGY_ID IS A REQUIRED PARAMETER' RETURN -1 END
IF @BDATE IS NULL
  BEGIN SELECT 'ERROR: @BDATE IS A REQUIRED PARAMETER' RETURN -1 END
IF @ACCOUNT_CD IS NULL
  BEGIN SELECT 'ERROR: @ACCOUNT_CD IS A REQUIRED PARAMETER' RETURN -1 END

CREATE TABLE #RESULT (
  security_id	int				NULL,
  ticker		varchar(16)		NULL,
  cusip			varchar(32)		NULL,
  sedol			varchar(32)		NULL,
  isin			varchar(64)		NULL,
  imnt_nm		varchar(255)	NULL,

  country_cd	varchar(4)		NULL,
  country_nm	varchar(128)	NULL,
  sector_id		int				NULL,
  sector_nm		varchar(64)		NULL,
  segment_id	int				NULL,
  segment_nm	varchar(128)	NULL,

  russell_sector_num	int				NULL,
  russell_sector_nm		varchar(64)		NULL,
  russell_industry_num	int				NULL,
  russell_industry_nm	varchar(255)	NULL,

  gics_sector_num		int				NULL,
  gics_sector_nm		varchar(64)		NULL,
  gics_segment_num		int				NULL,
  gics_segment_nm		varchar(128)	NULL,
  gics_industry_num		int				NULL,
  gics_industry_nm		varchar(255)	NULL,
  gics_sub_industry_num	int				NULL,
  gics_sub_industry_nm	varchar(255)	NULL,

  total_score		float		NULL,
  universe_score	float		NULL,
  country_score		float		NULL,
  ss_score			float		NULL,
  sector_score		float		NULL,
  segment_score		float		NULL,

  units		float		NULL,
  price		float		NULL,
  mval		float		NULL,
  
  account_wgt		float		NULL,
  benchmark_wgt		float		NULL,
  active_wgt		float		NULL
)

INSERT #RESULT
      (security_id, ticker, cusip, sedol, isin, imnt_nm, country_cd,
       russell_sector_num, russell_industry_num,
       gics_sector_num, gics_segment_num, gics_industry_num, gics_sub_industry_num,
       total_score, universe_score, country_score, ss_score, sector_score, segment_score)
SELECT y.security_id, y.ticker, y.cusip, y.sedol, y.isin, y.security_name, y.issue_country_cd,
       y.russell_sector_num, y.russell_industry_num,
       y.gics_sector_num, y.gics_industry_group_num, y.gics_industry_num, y.gics_sub_industry_num,
       s.total_score, s.universe_score, s.country_score, s.ss_score, s.sector_score, s.segment_score
  FROM scores s, equity_common..security y
 WHERE s.bdate = @BDATE
   AND s.strategy_id = @STRATEGY_ID
   AND s.security_id = y.security_id

INSERT #RESULT
      (security_id, ticker, cusip, sedol, isin, imnt_nm, country_cd,
       russell_sector_num, russell_industry_num,
       gics_sector_num, gics_segment_num, gics_industry_num, gics_sub_industry_num)
SELECT security_id, ticker, cusip, sedol, isin, security_name, issue_country_cd,
       russell_sector_num, russell_industry_num,
       gics_sector_num, gics_industry_group_num, gics_industry_num, gics_sub_industry_num
  FROM equity_common..security
 WHERE security_id IN (SELECT DISTINCT security_id
                         FROM equity_common..position
                        WHERE reference_date = @BDATE
                          AND reference_date = effective_date
                          AND acct_cd IN (SELECT DISTINCT acct_cd FROM equity_common..account WHERE parent = @ACCOUNT_CD OR acct_cd = @ACCOUNT_CD)
                          AND security_id IS NOT NULL
                          AND security_id NOT IN (SELECT security_id FROM #RESULT WHERE security_id IS NOT NULL))

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT: AFTER INITIAL INSERTS'
  SELECT * FROM #RESULT ORDER BY cusip, isin
END

UPDATE #RESULT
   SET sector_id = ss.sector_id,
       segment_id = ss.segment_id
  FROM sector_model_security ss, strategy g, factor_model f
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = f.factor_model_id
   AND ss.bdate = @BDATE
   AND ss.sector_model_id = f.sector_model_id
   AND #RESULT.security_id = ss.security_id

UPDATE #RESULT
   SET sector_nm = d.sector_nm
  FROM sector_def d
 WHERE #RESULT.sector_id = d.sector_id

UPDATE #RESULT
   SET segment_nm = d.segment_nm
  FROM segment_def d
 WHERE #RESULT.segment_id = d.segment_id

UPDATE #RESULT
   SET price = p.price_close_usd
  FROM equity_common..market_price p
 WHERE #RESULT.security_id = p.security_id
   AND p.reference_date = @BDATE

UPDATE #RESULT
   SET country_nm = UPPER(c.country_name)
  FROM equity_common..country c
 WHERE #RESULT.country_cd = c.country_cd

BEGIN--UPDATE SECTOR INFO
  UPDATE #RESULT
     SET russell_sector_nm = d.sector_nm
    FROM sector_model m, sector_def d
   WHERE m.sector_model_cd = 'RUSSELL-S'
     AND m.sector_model_id = d.sector_model_id
     AND d.sector_num = #RESULT.russell_sector_num

  UPDATE #RESULT
     SET russell_industry_nm = i.industry_nm
    FROM industry_model m, industry i
   WHERE m.industry_model_cd = 'RUSSELL-I'
     AND m.industry_model_id = i.industry_model_id
     AND i.industry_num = #RESULT.russell_industry_num

  UPDATE #RESULT
     SET gics_sector_nm = d.sector_nm
    FROM sector_model m, sector_def d
   WHERE m.sector_model_cd = 'GICS-S'
     AND m.sector_model_id = d.sector_model_id
     AND d.sector_num = #RESULT.gics_sector_num

  UPDATE #RESULT 
     SET gics_segment_nm = g.segment_nm
    FROM sector_model m, sector_def c, segment_def g
   WHERE m.sector_model_cd = 'GICS-S'
     AND m.sector_model_id = c.sector_model_id
     AND c.sector_id = g.sector_id
     AND g.segment_num = #RESULT.gics_segment_num

  UPDATE #RESULT
     SET gics_industry_nm = i.industry_nm
    FROM industry_model m, industry i
   WHERE m.industry_model_cd = 'GICS-I'
     AND m.industry_model_id = i.industry_model_id
     AND i.industry_num = #RESULT.gics_industry_num

  UPDATE #RESULT
     SET gics_sub_industry_nm = b.sub_industry_nm
    FROM industry_model m, industry i, sub_industry b
   WHERE m.industry_model_cd = 'GICS-I'
     AND m.industry_model_id = i.industry_model_id
     AND i.industry_id = b.industry_id
     AND b.sub_industry_num = #RESULT.gics_sub_industry_num
END--UPDATE SECTOR INFO

UPDATE #RESULT
   SET units = x.quantity
  FROM (SELECT security_id, SUM(ISNULL(quantity,0.0)) AS [quantity]
          FROM equity_common..position
         WHERE reference_date = @BDATE
           AND reference_date = effective_date
           AND acct_cd IN (SELECT DISTINCT acct_cd FROM equity_common..account WHERE parent = @ACCOUNT_CD OR acct_cd = @ACCOUNT_CD)
         GROUP BY security_id) x
 WHERE #RESULT.security_id = x.security_id

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT: AFTER INITIAL UPDATES'
  SELECT * FROM #RESULT ORDER BY cusip, isin
END

UPDATE #RESULT
   SET units = 0.0
 WHERE units IS NULL

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

IF EXISTS (SELECT 1 FROM account a, benchmark b
            WHERE a.strategy_id = @STRATEGY_ID
              AND a.account_cd = @ACCOUNT_CD
              AND a.benchmark_cd = b.benchmark_cd)
BEGIN
  UPDATE #RESULT
     SET benchmark_wgt = w.weight
    FROM account a, equity_common..benchmark_weight w
   WHERE a.strategy_id = @STRATEGY_ID
     AND a.account_cd = @ACCOUNT_CD
     AND a.benchmark_cd = w.acct_cd
     AND w.reference_date = @BDATE
     AND w.reference_date = w.effective_date
     AND #RESULT.security_id = w.security_id
END
ELSE
BEGIN
  UPDATE #RESULT
     SET benchmark_wgt = p.weight / 100.0
    FROM account a, universe_def d, universe_makeup p
   WHERE a.strategy_id = @STRATEGY_ID
     AND a.account_cd = @ACCOUNT_CD
     AND a.benchmark_cd = d.universe_cd
     AND d.universe_id = p.universe_id
     AND p.universe_dt = @BDATE
     AND #RESULT.security_id = p.security_id
END

UPDATE #RESULT
   SET benchmark_wgt = 0.0
 WHERE benchmark_wgt IS NULL

UPDATE #RESULT
   SET active_wgt = account_wgt - benchmark_wgt

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT: FINAL STATE'
  SELECT * FROM #RESULT ORDER BY cusip, isin
END

DECLARE @SQL varchar(1500)
SELECT @SQL = 'SELECT ticker AS [Ticker], '
SELECT @SQL = @SQL + 'cusip AS [CUSIP], '
SELECT @SQL = @SQL + 'sedol AS [SEDOL], '
SELECT @SQL = @SQL + 'isin AS [ISIN], '
SELECT @SQL = @SQL + 'imnt_nm AS [Name], '
SELECT @SQL = @SQL + 'country_nm AS [Country Name], '
SELECT @SQL = @SQL + 'sector_nm AS [Sector Name], '
SELECT @SQL = @SQL + 'segment_nm AS [Segment Name], '
SELECT @SQL = @SQL + 'russell_sector_nm AS [Russell Sector Name], '
SELECT @SQL = @SQL + 'russell_industry_nm AS [Russell Industry Name], '
SELECT @SQL = @SQL + 'gics_sector_nm AS [GICS Sector Name], '
SELECT @SQL = @SQL + 'gics_segment_nm AS [GICS Segment Name], '
SELECT @SQL = @SQL + 'gics_industry_nm AS [GICS Industry Name], '
SELECT @SQL = @SQL + 'gics_sub_industry_nm AS [GICS Sub-Industry Name], '
SELECT @SQL = @SQL + 'ROUND(total_score,1) AS [Total], '
SELECT @SQL = @SQL + 'ROUND(universe_score,1) AS [Universe], '
SELECT @SQL = @SQL + 'ROUND(country_score,1) AS [Country], '
SELECT @SQL = @SQL + 'ROUND(ss_score,1) AS [SS], '
SELECT @SQL = @SQL + 'ROUND(sector_score,1) AS [Sector], '
SELECT @SQL = @SQL + 'ROUND(segment_score,1) AS [Segment], '
SELECT @SQL = @SQL + 'account_wgt AS [Account], '
SELECT @SQL = @SQL + 'benchmark_wgt AS [Benchmark], '
SELECT @SQL = @SQL + 'active_wgt AS [Active] '
SELECT @SQL = @SQL + 'FROM #RESULT WHERE 1=1 '
IF @COUNTRY_CD IS NOT NULL
  BEGIN SELECT @SQL = @SQL + 'AND country_cd = ''' + @COUNTRY_CD + ''' ' END
IF @SECTOR_ID IS NOT NULL
  BEGIN SELECT @SQL = @SQL + 'AND sector_id = ' + CONVERT(varchar,@SECTOR_ID) + ' ' END
IF @SEGMENT_ID IS NOT NULL
  BEGIN SELECT @SQL = @SQL + 'AND sector_id = ' + CONVERT(varchar,@SEGMENT_ID) + ' ' END
IF EXISTS (SELECT * FROM decode WHERE item='MODEL SUMMARY - EXCLUDE NULL SECTOR' AND code=@STRATEGY_ID)
  BEGIN SELECT @SQL = @SQL + 'AND sector_id IS NOT NULL ' END
IF EXISTS (SELECT * FROM strategy WHERE strategy_id = @STRATEGY_ID AND rank_order = 1)
  BEGIN SELECT @SQL = @SQL + 'ORDER BY total_score DESC, universe_score DESC, ss_score DESC, country_score DESC, ticker, cusip, isin' END
ELSE
  BEGIN SELECT @SQL = @SQL + 'ORDER BY ISNULL(total_score,9999), ISNULL(universe_score,9999), ISNULL(ss_score,9999), ISNULL(country_score,9999), ticker, cusip, isin' END

IF @DEBUG = 1
  BEGIN SELECT '@SQL', @SQL END

EXEC(@SQL)

DROP TABLE #RESULT

RETURN 0
go
IF OBJECT_ID('dbo.rpt_model_summary') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_model_summary >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_model_summary >>>'
go
