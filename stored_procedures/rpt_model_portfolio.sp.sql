use QER
go
IF OBJECT_ID('dbo.rpt_model_portfolio') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rpt_model_portfolio
    IF OBJECT_ID('dbo.rpt_model_portfolio') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rpt_model_portfolio >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rpt_model_portfolio >>>'
END
go
CREATE PROCEDURE dbo.rpt_model_portfolio @STRATEGY_ID int,
                                         @BDATE datetime,
                                         @ACCOUNT_CD varchar(32),
                                         @MODEL_WEIGHT varchar(16),
                                         @COUNTRY_CD varchar(4) = NULL,
                                         @SECTOR_ID int = NULL,
                                         @SEGMENT_ID int = NULL,
                                         @DEBUG bit = NULL
AS
/* MODEL - PORTFOLIO */

/****
* KNOWN ISSUES:
*   THIS PROCEDURE DOES NOT HANDLE INTERNATIONAL SECURITIES - IT JOINS ON CUSIP ONLY
****/

IF @STRATEGY_ID IS NULL
  BEGIN SELECT 'ERROR: @STRATEGY_ID IS A REQUIRED PARAMETER' RETURN -1 END
IF @BDATE IS NULL
  BEGIN SELECT 'ERROR: @BDATE IS A REQUIRED PARAMETER' RETURN -1 END
IF @ACCOUNT_CD IS NULL
  BEGIN SELECT 'ERROR: @ACCOUNT_CD IS A REQUIRED PARAMETER' RETURN -1 END
IF @MODEL_WEIGHT IS NULL
  BEGIN SELECT 'ERROR: @MODEL_WEIGHT IS A REQUIRED PARAMETER' RETURN -1 END
IF @MODEL_WEIGHT NOT IN ('CAP', 'EQUAL')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @MODEL_WEIGHT PARAMETER' RETURN -1 END

CREATE TABLE #RESULT (
  mqa_id		varchar(32)	NULL,
  ticker		varchar(16)	NULL,
  cusip			varchar(32)	NULL,
  sedol			varchar(32)	NULL,
  isin			varchar(64)	NULL,
  imnt_nm		varchar(255)	NULL,

  country_cd		varchar(4)	NULL,
  country_nm		varchar(128)	NULL,
  sector_id		int		NULL,
  sector_nm		varchar(64)	NULL,
  segment_id		int		NULL,
  segment_nm		varchar(128)	NULL,

  russell_sector_num	int		NULL,
  russell_sector_nm	varchar(64)	NULL,
  russell_industry_num	int		NULL,
  russell_industry_nm	varchar(255)	NULL,

  gics_sector_num	int		NULL,
  gics_sector_nm	varchar(64)	NULL,
  gics_segment_num	int		NULL,
  gics_segment_nm	varchar(128)	NULL,
  gics_industry_num	int		NULL,
  gics_industry_nm	varchar(255)	NULL,
  gics_sub_industry_num	int		NULL,
  gics_sub_industry_nm	varchar(255)	NULL,

  total_score		float		NULL,
  universe_score	float		NULL,
  country_score		float		NULL,
  ss_score		float		NULL,
  sector_score		float		NULL,
  segment_score		float		NULL,

  units			float		NULL,
  price			float		NULL,
  mval			float		NULL,
  
  account_wgt		float		NULL,
  benchmark_wgt		float		NULL,
  model_wgt		float		NULL,

  acct_bm_wgt		float		NULL,
  mpf_bm_wgt		float		NULL,
  acct_mpf_wgt		float		NULL
)

DECLARE @MODEL_ID int
IF @MODEL_WEIGHT='CAP'
BEGIN
  SELECT @MODEL_ID = d2.universe_id
    FROM strategy g, universe_def d1, universe_def d2
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.universe_id = d1.universe_id
     AND d2.universe_cd = d1.universe_cd + '_MPF_CAP'
END
ELSE IF @MODEL_WEIGHT='EQUAL'
BEGIN
  SELECT @MODEL_ID = d2.universe_id
    FROM strategy g, universe_def d1, universe_def d2
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.universe_id = d1.universe_id
     AND d2.universe_cd = d1.universe_cd + '_MPF_EQL'
END

INSERT #RESULT
      (mqa_id, ticker, cusip, sedol, isin,
       total_score, universe_score, country_score, ss_score, sector_score, segment_score, model_wgt)
SELECT s.mqa_id, s.ticker, s.cusip, s.sedol, s.isin,
       s.total_score, s.universe_score, s.country_score, s.ss_score, s.sector_score, s.segment_score, p.weight / 100.0
  FROM scores s, universe_makeup p
 WHERE s.bdate = @BDATE
   AND s.strategy_id = @STRATEGY_ID
   AND p.universe_id = @MODEL_ID
   AND p.universe_dt = @BDATE
   AND s.cusip = p.cusip

INSERT #RESULT
      (mqa_id, ticker, cusip, sedol, isin)
SELECT p.mqa_id, p.ticker, p.cusip, p.sedol, p.isin
  FROM position p
 WHERE p.bdate = @BDATE
   AND p.account_cd = @ACCOUNT_CD
   AND p.cusip IS NOT NULL
   AND p.cusip NOT IN (SELECT cusip FROM #RESULT WHERE cusip IS NOT NULL)

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT: AFTER INITIAL INSERT'
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
   AND #RESULT.cusip = ss.cusip

UPDATE #RESULT
   SET sector_nm = d.sector_nm
  FROM sector_def d
 WHERE #RESULT.sector_id = d.sector_id

UPDATE #RESULT
   SET segment_nm = d.segment_nm
  FROM segment_def d
 WHERE #RESULT.segment_id = d.segment_id

UPDATE #RESULT
   SET imnt_nm = i.imnt_nm,
       country_cd = i.country,
       price = i.price_close,
       russell_sector_num = i.russell_sector_num,
       russell_industry_num = i.russell_industry_num,
       gics_sector_num = i.gics_sector_num,
       gics_segment_num = i.gics_segment_num,
       gics_industry_num = i.gics_industry_num,
       gics_sub_industry_num = i.gics_sub_industry_num
  FROM instrument_characteristics i
 WHERE i.bdate = @BDATE
   AND #RESULT.cusip = i.cusip

UPDATE #RESULT
   SET country_nm = d.decode
  FROM decode d
 WHERE d.item = 'COUNTRY'
   AND #RESULT.country_cd = d.code

UPDATE #RESULT
   SET imnt_nm = 'CASH',
       country_cd = 'US',
       country_nm = 'UNITED STATES',
       price = 1.0
 WHERE cusip = '_USD'

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
   SET units = p.units
  FROM position p
 WHERE p.bdate = @BDATE
   AND p.account_cd = @ACCOUNT_CD
   AND #RESULT.cusip = p.cusip

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

UPDATE #RESULT
   SET benchmark_wgt = p.weight / 100.0
  FROM account a, universe_makeup p
 WHERE a.strategy_id = @STRATEGY_ID
   AND a.account_cd = @ACCOUNT_CD
   AND a.bm_universe_id = p.universe_id
   AND p.universe_dt = @BDATE
   AND #RESULT.cusip = p.cusip

UPDATE #RESULT
   SET benchmark_wgt = 0.0
 WHERE benchmark_wgt IS NULL

IF EXISTS (SELECT * FROM decode WHERE item='MODEL WEIGHT NULL' AND code=@STRATEGY_ID)
  BEGIN UPDATE #RESULT SET model_wgt = NULL END

UPDATE #RESULT
   SET acct_bm_wgt = account_wgt - benchmark_wgt,
       mpf_bm_wgt = model_wgt - benchmark_wgt,
       acct_mpf_wgt = account_wgt - model_wgt

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT: FINAL STATE'
  SELECT * FROM #RESULT ORDER BY cusip, isin
END

IF EXISTS (SELECT * FROM strategy WHERE strategy_id = @STRATEGY_ID AND rank_order = 1)
BEGIN
  SELECT ticker		AS [Ticker],
         cusip		AS [CUSIP],
         sedol		AS [SEDOL],
         isin		AS [ISIN],
         imnt_nm	AS [Name],
         country_nm		AS [Country Name],
         sector_nm		AS [Sector Name],
         segment_nm		AS [Segment Name],
         russell_sector_nm		AS [Russell Sector Name],
         russell_industry_nm	AS [Russell Industry Name],
         gics_sector_nm			AS [GICS Sector Name],
         gics_segment_nm		AS [GICS Segment Name],
         gics_industry_nm		AS [GICS Industry Name],
         gics_sub_industry_nm	AS [GICS Sub-Industry Name],
         ROUND(total_score,1)		AS [Total],
         ROUND(universe_score,1)	AS [Universe],
         ROUND(country_score,1)		AS [Country],
         ROUND(ss_score,1)			AS [SS],
         ROUND(sector_score,1)		AS [Sector],
         ROUND(segment_score,1)		AS [Segment],
         account_wgt	AS [Account],
         benchmark_wgt	AS [Benchmark],
         model_wgt		AS [Model],
         acct_bm_wgt	AS [Acct-Bmk],
         mpf_bm_wgt		AS [Model-Bmk],
         acct_mpf_wgt	AS [Acct-Model]
    FROM #RESULT
   WHERE total_score IS NOT NULL
     AND (country_cd = @COUNTRY_CD OR @COUNTRY_CD IS NULL)
     AND (sector_id = @SECTOR_ID OR @SECTOR_ID IS NULL)
     AND (segment_id = @SEGMENT_ID OR @SEGMENT_ID IS NULL)
   ORDER BY total_score DESC, universe_score DESC, ss_score DESC, country_score DESC, ticker, cusip, isin
END
ELSE
BEGIN
  SELECT ticker		AS [Ticker],
         cusip		AS [CUSIP],
         sedol		AS [SEDOL],
         isin		AS [ISIN],
         imnt_nm	AS [Name],
         country_nm		AS [Country Name],
         sector_nm		AS [Sector Name],
         segment_nm		AS [Segment Name],
         russell_sector_nm		AS [Russell Sector Name],
         russell_industry_nm	AS [Russell Industry Name],
         gics_sector_nm			AS [GICS Sector Name],
         gics_segment_nm		AS [GICS Segment Name],
         gics_industry_nm		AS [GICS Industry Name],
         gics_sub_industry_nm	AS [GICS Sub-Industry Name],
         ROUND(total_score,1)		AS [Total],
         ROUND(universe_score,1)	AS [Universe],
         ROUND(country_score,1)		AS [Country],
         ROUND(ss_score,1)			AS [SS],
         ROUND(sector_score,1)		AS [Sector],
         ROUND(segment_score,1)		AS [Segment],
         account_wgt	AS [Account],
         benchmark_wgt	AS [Benchmark],
         model_wgt		AS [Model],
         acct_bm_wgt	AS [Acct-Bmk],
         mpf_bm_wgt		AS [Model-Bmk],
         acct_mpf_wgt	AS [Acct-Model]
    FROM #RESULT
   WHERE total_score IS NOT NULL
     AND (country_cd = @COUNTRY_CD OR @COUNTRY_CD IS NULL)
     AND (sector_id = @SECTOR_ID OR @SECTOR_ID IS NULL)
     AND (segment_id = @SEGMENT_ID OR @SEGMENT_ID IS NULL)
   ORDER BY ISNULL(total_score,9999), ISNULL(universe_score,9999), ISNULL(ss_score,9999), ISNULL(country_score,9999), ticker, cusip, isin
END

DROP TABLE #RESULT

RETURN 0
go
IF OBJECT_ID('dbo.rpt_model_portfolio') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_model_portfolio >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_model_portfolio >>>'
go
