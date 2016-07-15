use QER
go
IF OBJECT_ID('dbo.rpt_model_performance') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rpt_model_performance
    IF OBJECT_ID('dbo.rpt_model_performance') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rpt_model_performance >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rpt_model_performance >>>'
END
go
CREATE PROCEDURE dbo.rpt_model_performance @BDATE datetime,
                                           @RETURN_TYPE varchar(16),
                                           @STRATEGY_ID int,
                                           @ACCOUNT_CD varchar(32) = NULL,
                                           @BM_UNIVERSE_ID int = NULL,
                                           @MODEL_PORTFOLIO_DEF_CD varchar(32) = NULL,
                                           @WEIGHT varchar(16) = NULL,
                                           @PERIOD_TYPE varchar(2),
                                           @PERIODS int,
                                           @DEBUG bit = NULL
AS
/* MODEL - PERFORMANCE */

/****
* KNOWN ISSUES:
* - THIS PROCEDURE DOES NOT HANDLE INTERNATIONAL SECURITIES - IT JOINS ON CUSIP ONLY
* - THIS PROCEDURE DOES NOT HANDLE "SEGMENT MODELS"
* - IF RETURN_TYPE LIKE '%MODEL%' AND THERE IS A CUSIP CHANGE DURING THE PERIOD,
*   THE SECURITY'S RETURN AFTER THE CUSIP CHANGE FOR THAT PERIOD WILL NOT BE CAPTURED
****/

IF @BDATE IS NULL
  BEGIN SELECT 'ERROR: @BDATE IS A REQUIRED PARAMETER' RETURN -1 END
IF @RETURN_TYPE IS NULL
  BEGIN SELECT 'ERROR: @RETURN_TYPE IS A REQUIRED PARAMETER' RETURN -1 END
IF @RETURN_TYPE NOT IN ('ACCT', 'BMK', 'MODEL', 'ACCT-BMK', 'MODEL-BMK', 'ACCT-MODEL')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @RETURN_TYPE PARAMETER' RETURN -1 END
IF @RETURN_TYPE != 'ACCT' AND @WEIGHT IS NULL
  BEGIN SELECT 'ERROR: @WEIGHT IS A REQUIRED PARAMETER' RETURN -1 END
IF @RETURN_TYPE LIKE '%MODEL%' AND @MODEL_PORTFOLIO_DEF_CD IS NULL
  BEGIN SELECT 'ERROR: @MODEL_PORTFOLIO_DEF_CD IS A REQUIRED PARAMETER' RETURN -1 END
IF @RETURN_TYPE LIKE '%ACCT%' AND @ACCOUNT_CD IS NULL
  BEGIN SELECT 'ERROR: @ACCOUNT_CD IS A REQUIRED PARAMETER' RETURN -1 END
IF @RETURN_TYPE LIKE '%BMK%' AND @BM_UNIVERSE_ID IS NULL
  BEGIN SELECT 'ERROR: @BM_UNIVERSE_ID IS A REQUIRED PARAMETER' RETURN -1 END
IF @STRATEGY_ID IS NULL
  BEGIN SELECT 'ERROR: @STRATEGY_ID IS A REQUIRED PARAMETER' RETURN -1 END
IF @PERIOD_TYPE IS NULL
  BEGIN SELECT 'ERROR: @PERIOD_TYPE IS A REQUIRED PARAMETER' RETURN -1 END
IF @PERIOD_TYPE NOT IN ('YY', 'YYYY', 'QQ', 'Q', 'MM', 'M', 'WK', 'WW', 'DD', 'D')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @PERIOD_TYPE PARAMETER' RETURN -1 END
IF @PERIODS IS NULL
  BEGIN SELECT 'ERROR: @PERIODS IS A REQUIRED PARAMETER' RETURN -1 END
IF @PERIODS > 0
  BEGIN SELECT @PERIODS = -1 * @PERIODS END
IF @PERIODS != 0
  BEGIN SELECT @PERIODS = @PERIODS + 1 END

DECLARE @ADATE datetime,
        @CDATE datetime,
        @BEGIN_BDATE datetime,
        @END_BDATE datetime,
        @MIN_PERIOD_BDATE datetime,
        @MODEL_DEF_CD varchar(32),
        @PERIOD_ID int,
        @PERIOD_NUM int,
        @MINUS_INDEX int

CREATE TABLE #RESULT (
  ordinal	int		NULL,
  period	varchar(32)	NULL,
  begin_bdate	datetime	NULL,
  end_bdate	datetime	NULL,

  acct_rtn	float		NULL,
  bmk_rtn	float		NULL,
  ml_rtn	float		NULL,
  ms_rtn	float		NULL,

  total_rtn	float		NULL,
  universe_rtn	float		NULL,
  sector_rtn	float		NULL
)

SELECT @ADATE = CONVERT(varchar, DATEPART(YY, @BDATE)) + '0101'
EXEC business_date_get @DIFF=0, @REF_DATE=@ADATE, @RET_DATE=@ADATE OUTPUT

INSERT #RESULT (ordinal, period, begin_bdate, end_bdate)
SELECT 1, 'YTD', @ADATE, @BDATE

SELECT @ADATE = @BDATE
SELECT @PERIOD_NUM = DATEPART(QQ, @BDATE)
WHILE @PERIOD_NUM = DATEPART(QQ, @ADATE)
  BEGIN SELECT @ADATE = DATEADD(DD, -1, @ADATE) END
SELECT @ADATE = DATEADD(DD, 1, @ADATE)
EXEC business_date_get @DIFF=0, @REF_DATE=@ADATE, @RET_DATE=@ADATE OUTPUT

INSERT #RESULT (ordinal, period, begin_bdate, end_bdate)
SELECT 2, 'QTD', @ADATE, @BDATE

SELECT @ADATE = @BDATE
SELECT @PERIOD_NUM = DATEPART(MM, @BDATE)
WHILE @PERIOD_NUM = DATEPART(MM, @ADATE)
  BEGIN SELECT @ADATE = DATEADD(DD, -1, @ADATE) END
SELECT @ADATE = DATEADD(DD, 1, @ADATE)
EXEC business_date_get @DIFF=0, @REF_DATE=@ADATE, @RET_DATE=@ADATE OUTPUT

INSERT #RESULT (ordinal, period, begin_bdate, end_bdate)
SELECT 3, 'MTD', @ADATE, @BDATE

SELECT @ADATE = @BDATE
WHILE DATEPART(DW, @ADATE) != 1
  BEGIN SELECT @ADATE = DATEADD(DD, -1, @ADATE) END
EXEC business_date_get @DIFF=0, @REF_DATE=@ADATE, @RET_DATE=@ADATE OUTPUT

INSERT #RESULT (ordinal, period, begin_bdate, end_bdate)
SELECT 4, 'WTD', @ADATE, @BDATE

IF @PERIOD_TYPE IN ('YY','YYYY')
BEGIN
  SELECT @MIN_PERIOD_BDATE = DATEADD(YY, @PERIODS, @BDATE)
  SELECT @PERIOD_NUM = DATEPART(YY, @MIN_PERIOD_BDATE)
  WHILE @PERIOD_NUM = DATEPART(YY, @MIN_PERIOD_BDATE)
    BEGIN SELECT @MIN_PERIOD_BDATE = DATEADD(DD, -1, @MIN_PERIOD_BDATE) END
  SELECT @MIN_PERIOD_BDATE = DATEADD(DD, 1, @MIN_PERIOD_BDATE)
  EXEC business_date_get @DIFF=0, @REF_DATE=@MIN_PERIOD_BDATE, @RET_DATE=@MIN_PERIOD_BDATE OUTPUT
  SELECT @ADATE = @MIN_PERIOD_BDATE
  SELECT @PERIOD_NUM = DATEPART(YY, @MIN_PERIOD_BDATE)
  WHILE @PERIOD_NUM = DATEPART(YY, @ADATE)
    BEGIN SELECT @ADATE = DATEADD(DD, 1, @ADATE) END
END
ELSE IF @PERIOD_TYPE IN ('QQ','Q')
BEGIN
  SELECT @MIN_PERIOD_BDATE = DATEADD(QQ, @PERIODS, @BDATE)
  SELECT @PERIOD_NUM = DATEPART(QQ, @MIN_PERIOD_BDATE)
  WHILE @PERIOD_NUM = DATEPART(QQ, @MIN_PERIOD_BDATE)
    BEGIN SELECT @MIN_PERIOD_BDATE = DATEADD(DD, -1, @MIN_PERIOD_BDATE) END
  SELECT @MIN_PERIOD_BDATE = DATEADD(DD, 1, @MIN_PERIOD_BDATE)
  EXEC business_date_get @DIFF=0, @REF_DATE=@MIN_PERIOD_BDATE, @RET_DATE=@MIN_PERIOD_BDATE OUTPUT
  SELECT @ADATE = @MIN_PERIOD_BDATE
  SELECT @PERIOD_NUM = DATEPART(QQ, @MIN_PERIOD_BDATE)
  WHILE @PERIOD_NUM = DATEPART(QQ, @ADATE)
    BEGIN SELECT @ADATE = DATEADD(DD, 1, @ADATE) END
END
ELSE IF @PERIOD_TYPE IN ('MM','M')
BEGIN
  SELECT @MIN_PERIOD_BDATE = DATEADD(MM, @PERIODS, @BDATE)
  SELECT @PERIOD_NUM = DATEPART(MM, @MIN_PERIOD_BDATE)
  WHILE @PERIOD_NUM = DATEPART(MM, @MIN_PERIOD_BDATE)
    BEGIN SELECT @MIN_PERIOD_BDATE = DATEADD(DD, -1, @MIN_PERIOD_BDATE) END
  SELECT @MIN_PERIOD_BDATE = DATEADD(DD, 1, @MIN_PERIOD_BDATE)
  EXEC business_date_get @DIFF=0, @REF_DATE=@MIN_PERIOD_BDATE, @RET_DATE=@MIN_PERIOD_BDATE OUTPUT
  SELECT @ADATE = @MIN_PERIOD_BDATE
  SELECT @PERIOD_NUM = DATEPART(MM, @MIN_PERIOD_BDATE)
  WHILE @PERIOD_NUM = DATEPART(MM, @ADATE)
    BEGIN SELECT @ADATE = DATEADD(DD, 1, @ADATE) END
END
ELSE IF @PERIOD_TYPE IN ('WK','WW')
BEGIN
  SELECT @MIN_PERIOD_BDATE = DATEADD(WK, @PERIODS, @BDATE)
  SELECT @PERIOD_NUM = DATEPART(WK, @MIN_PERIOD_BDATE)
  WHILE @PERIOD_NUM = DATEPART(WK, @MIN_PERIOD_BDATE)
    BEGIN SELECT @MIN_PERIOD_BDATE = DATEADD(DD, -1, @MIN_PERIOD_BDATE) END
  SELECT @MIN_PERIOD_BDATE = DATEADD(DD, 1, @MIN_PERIOD_BDATE)
  EXEC business_date_get @DIFF=0, @REF_DATE=@MIN_PERIOD_BDATE, @RET_DATE=@MIN_PERIOD_BDATE OUTPUT
  SELECT @ADATE = @MIN_PERIOD_BDATE
  SELECT @PERIOD_NUM = DATEPART(WW, @MIN_PERIOD_BDATE)
  WHILE @PERIOD_NUM = DATEPART(WW, @ADATE)
    BEGIN SELECT @ADATE = DATEADD(DD, 1, @ADATE) END
END
ELSE IF @PERIOD_TYPE IN ('DD','D')
BEGIN
  SELECT @MIN_PERIOD_BDATE = DATEADD(DD, @PERIODS, @BDATE)
  EXEC business_date_get @DIFF=0, @REF_DATE=@MIN_PERIOD_BDATE, @RET_DATE=@MIN_PERIOD_BDATE OUTPUT
  SELECT @ADATE = @MIN_PERIOD_BDATE
END

IF @PERIOD_TYPE NOT IN ('DD','D')
  BEGIN EXEC business_date_get @DIFF=-1, @REF_DATE=@ADATE, @RET_DATE=@ADATE OUTPUT END

IF @ADATE > @BDATE
  BEGIN INSERT #RESULT (begin_bdate, end_bdate) VALUES (@MIN_PERIOD_BDATE, @BDATE) END
ELSE
  BEGIN INSERT #RESULT (begin_bdate, end_bdate) VALUES (@MIN_PERIOD_BDATE, @ADATE) END

WHILE NOT EXISTS (SELECT * FROM #RESULT WHERE end_bdate >= @BDATE AND ordinal IS NULL)
BEGIN
  SELECT @CDATE = MAX(end_bdate) FROM #RESULT WHERE ordinal IS NULL
  EXEC business_date_get @DIFF=1, @REF_DATE=@CDATE, @RET_DATE=@CDATE OUTPUT
  SELECT @ADATE = @CDATE

  IF @PERIOD_TYPE IN ('YY','YYYY')
  BEGIN
    SELECT @PERIOD_NUM = DATEPART(YY, @CDATE)
    WHILE @PERIOD_NUM = DATEPART(YY, @ADATE)
      BEGIN SELECT @ADATE = DATEADD(DD, 1, @ADATE) END
  END
  ELSE IF @PERIOD_TYPE IN ('QQ','Q')
  BEGIN
    SELECT @PERIOD_NUM = DATEPART(QQ, @CDATE)
    WHILE @PERIOD_NUM = DATEPART(QQ, @ADATE)
      BEGIN SELECT @ADATE = DATEADD(DD, 1, @ADATE) END
  END
  ELSE IF @PERIOD_TYPE IN ('MM','M')
  BEGIN
    SELECT @PERIOD_NUM = DATEPART(MM, @CDATE)
    WHILE @PERIOD_NUM = DATEPART(MM, @ADATE)
      BEGIN SELECT @ADATE = DATEADD(DD, 1, @ADATE) END
  END
  ELSE IF @PERIOD_TYPE IN ('WK','WW')
  BEGIN
    SELECT @PERIOD_NUM = DATEPART(WW, @CDATE)
    WHILE @PERIOD_NUM = DATEPART(WW, @ADATE)
      BEGIN SELECT @ADATE = DATEADD(DD, 1, @ADATE) END
  END

  IF @PERIOD_TYPE NOT IN ('DD','D')
    BEGIN EXEC business_date_get @DIFF=-1, @REF_DATE=@ADATE, @RET_DATE=@ADATE OUTPUT END

  IF @ADATE > @BDATE
    BEGIN INSERT #RESULT (begin_bdate, end_bdate) VALUES (@CDATE, @BDATE) END
  ELSE
    BEGIN INSERT #RESULT (begin_bdate, end_bdate) VALUES (@CDATE, @ADATE) END
END

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (1)'
  SELECT * FROM #RESULT ORDER BY ordinal, end_bdate
END

CREATE TABLE #RTN_PERIOD (
  period_id	int identity(1,1)	NOT NULL,
  begin_bdate	datetime		NULL,
  end_bdate	datetime		NULL
)

INSERT #RTN_PERIOD (begin_bdate, end_bdate)
SELECT DISTINCT begin_bdate, end_bdate FROM #RESULT
ORDER BY begin_bdate, end_bdate

IF @DEBUG = 1
BEGIN
  SELECT '#RTN_PERIOD'
  SELECT * FROM #RTN_PERIOD ORDER BY begin_bdate, end_bdate
END

/**** CHECK/RUN RETURNS: BEGIN ****/
SELECT @PERIOD_ID = 0
WHILE EXISTS (SELECT * FROM #RTN_PERIOD WHERE period_id > @PERIOD_ID)
BEGIN
  SELECT @PERIOD_ID = MIN(period_id) FROM #RTN_PERIOD WHERE period_id > @PERIOD_ID
  SELECT @BEGIN_BDATE=begin_bdate, @END_BDATE=end_bdate FROM #RTN_PERIOD WHERE period_id=@PERIOD_ID
  IF @RETURN_TYPE LIKE '%ACCT%'
  BEGIN
    IF NOT EXISTS (SELECT r.* FROM return_calc_params p, return_calc_result r
                    WHERE p.bdate_from = @BEGIN_BDATE
                      AND p.bdate_to = @END_BDATE
                      AND p.return_type = 'ACCT'
                      AND p.strategy_id = @STRATEGY_ID
                      AND p.weight = 'MVAL'
                      AND p.account_cd = @ACCOUNT_CD
                      AND p.return_calc_id = r.return_calc_id)
    BEGIN
      DELETE return_calc_params
       WHERE bdate_from = @BEGIN_BDATE
         AND bdate_to = @END_BDATE
         AND return_type = 'ACCT'
         AND strategy_id = @STRATEGY_ID
         AND weight = 'MVAL'
         AND account_cd = @ACCOUNT_CD
      EXEC return_calculate @BDATE_FROM=@BEGIN_BDATE, @BDATE_TO=@END_BDATE, @RETURN_TYPE='ACCT', @STRATEGY_ID=@STRATEGY_ID, @ACCOUNT_CD=@ACCOUNT_CD, @DEBUG=@DEBUG
    END
  END
  IF @RETURN_TYPE LIKE '%BMK%'
  BEGIN
    IF NOT EXISTS (SELECT r.* FROM return_calc_params p, return_calc_result r
                    WHERE p.bdate_from = @BEGIN_BDATE
                      AND p.bdate_to = @END_BDATE
                      AND p.return_type = 'BMK'
                      AND p.strategy_id = @STRATEGY_ID
                      AND p.weight = @WEIGHT
                      AND p.bm_universe_id = @BM_UNIVERSE_ID
                      AND p.return_calc_id = r.return_calc_id)
    BEGIN
      DELETE return_calc_params
       WHERE bdate_from = @BEGIN_BDATE
         AND bdate_to = @END_BDATE
         AND return_type = 'BMK'
         AND strategy_id = @STRATEGY_ID
         AND weight = @WEIGHT
         AND bm_universe_id = @BM_UNIVERSE_ID
      EXEC return_calculate @BDATE_FROM=@BEGIN_BDATE, @BDATE_TO=@END_BDATE, @RETURN_TYPE='BMK', @STRATEGY_ID=@STRATEGY_ID, @WEIGHT=@WEIGHT, @BM_UNIVERSE_ID=@BM_UNIVERSE_ID, @DEBUG=@DEBUG
    END
  END
  IF @RETURN_TYPE LIKE '%MODEL%'
  BEGIN
    IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
    BEGIN--LONG-SHORT
      SELECT @MINUS_INDEX = CHARINDEX('-', @MODEL_PORTFOLIO_DEF_CD, 1)
      SELECT @MODEL_DEF_CD = SUBSTRING(@MODEL_PORTFOLIO_DEF_CD, 1, @MINUS_INDEX-1)
      IF NOT EXISTS (SELECT r.* FROM return_calc_params p, return_calc_result r
                      WHERE p.bdate_from = @BEGIN_BDATE
                        AND p.bdate_to = @END_BDATE
                        AND p.return_type = 'MODEL'
                        AND p.strategy_id = @STRATEGY_ID
                        AND p.weight = @WEIGHT
                        AND p.model_def_cd = @MODEL_DEF_CD
                        AND p.return_calc_id = r.return_calc_id)
      BEGIN
        DELETE return_calc_params
         WHERE bdate_from = @BEGIN_BDATE
           AND bdate_to = @END_BDATE
           AND return_type = 'MODEL'
           AND strategy_id = @STRATEGY_ID
           AND weight = @WEIGHT
           AND model_def_cd = @MODEL_DEF_CD
        EXEC return_calculate @BDATE_FROM=@BEGIN_BDATE, @BDATE_TO=@END_BDATE, @RETURN_TYPE='MODEL', @STRATEGY_ID=@STRATEGY_ID, @WEIGHT=@WEIGHT, @MODEL_DEF_CD=@MODEL_DEF_CD, @DEBUG=@DEBUG
      END
      SELECT @MODEL_DEF_CD = SUBSTRING(@MODEL_PORTFOLIO_DEF_CD, @MINUS_INDEX+1, LEN(@MODEL_PORTFOLIO_DEF_CD))
      IF NOT EXISTS (SELECT r.* FROM return_calc_params p, return_calc_result r
                      WHERE p.bdate_from = @BEGIN_BDATE
                        AND p.bdate_to = @END_BDATE
                        AND p.return_type = 'MODEL'
                        AND p.strategy_id = @STRATEGY_ID
                        AND p.weight = @WEIGHT
                        AND p.model_def_cd = @MODEL_DEF_CD
                        AND p.return_calc_id = r.return_calc_id)
      BEGIN
        DELETE return_calc_params
         WHERE bdate_from = @BEGIN_BDATE
           AND bdate_to = @END_BDATE
           AND return_type = 'MODEL'
           AND strategy_id = @STRATEGY_ID
           AND weight = @WEIGHT
           AND model_def_cd = @MODEL_DEF_CD
        EXEC return_calculate @BDATE_FROM=@BEGIN_BDATE, @BDATE_TO=@END_BDATE, @RETURN_TYPE='MODEL', @STRATEGY_ID=@STRATEGY_ID, @WEIGHT=@WEIGHT, @MODEL_DEF_CD=@MODEL_DEF_CD, @DEBUG=@DEBUG
      END
    END
    ELSE
    BEGIN--LONG ONLY
      IF NOT EXISTS (SELECT r.* FROM return_calc_params p, return_calc_result r
                      WHERE p.bdate_from = @BEGIN_BDATE
                        AND p.bdate_to = @END_BDATE
                        AND p.return_type = 'MODEL'
                        AND p.strategy_id = @STRATEGY_ID
                        AND p.weight = @WEIGHT
                        AND p.model_def_cd = @MODEL_PORTFOLIO_DEF_CD
                        AND p.return_calc_id = r.return_calc_id)
      BEGIN
        DELETE return_calc_params
         WHERE bdate_from = @BEGIN_BDATE
           AND bdate_to = @END_BDATE
           AND return_type = 'MODEL'
           AND strategy_id = @STRATEGY_ID
           AND weight = @WEIGHT
           AND model_def_cd = @MODEL_PORTFOLIO_DEF_CD
        EXEC return_calculate @BDATE_FROM=@BEGIN_BDATE, @BDATE_TO=@END_BDATE, @RETURN_TYPE='MODEL', @STRATEGY_ID=@STRATEGY_ID, @WEIGHT=@WEIGHT, @MODEL_DEF_CD=@MODEL_PORTFOLIO_DEF_CD, @DEBUG=@DEBUG
      END
    END
  END
END
/**** CHECK/RUN RETURNS: END ****/

/**** UPDATE TOTAL RETURN: BEGIN ****/
UPDATE #RESULT SET acct_rtn=0.0, bmk_rtn=0.0, ml_rtn=0.0, ms_rtn=0.0
SELECT @PERIOD_ID = 0
WHILE EXISTS (SELECT * FROM #RTN_PERIOD WHERE period_id > @PERIOD_ID)
BEGIN
  SELECT @PERIOD_ID = MIN(period_id) FROM #RTN_PERIOD WHERE period_id > @PERIOD_ID
  SELECT @BEGIN_BDATE=begin_bdate, @END_BDATE=end_bdate FROM #RTN_PERIOD WHERE period_id=@PERIOD_ID
  IF @RETURN_TYPE LIKE '%ACCT%'
  BEGIN
    UPDATE #RESULT SET acct_rtn = r.rtn
      FROM return_calc_params p, return_calc_result r
     WHERE #RESULT.begin_bdate = @BEGIN_BDATE
       AND #RESULT.end_bdate = @END_BDATE
       AND #RESULT.begin_bdate = p.bdate_from
       AND #RESULT.end_bdate = p.bdate_to
       AND p.return_type = 'ACCT'
       AND p.strategy_id = @STRATEGY_ID
       AND p.weight = 'MVAL'
       AND p.account_cd = @ACCOUNT_CD
       AND p.return_calc_id = r.return_calc_id
       AND r.univ_type = 'TOTAL'
  END
  IF @RETURN_TYPE LIKE '%BMK%'
  BEGIN
    UPDATE #RESULT SET bmk_rtn = r.rtn
      FROM return_calc_params p, return_calc_result r
     WHERE #RESULT.begin_bdate = @BEGIN_BDATE
       AND #RESULT.end_bdate = @END_BDATE
       AND #RESULT.begin_bdate = p.bdate_from
       AND #RESULT.end_bdate = p.bdate_to
       AND p.return_type = 'BMK'
       AND p.strategy_id = @STRATEGY_ID
       AND p.weight = @WEIGHT
       AND p.bm_universe_id = @BM_UNIVERSE_ID
       AND p.return_calc_id = r.return_calc_id
       AND r.univ_type = 'TOTAL'
  END
  IF @RETURN_TYPE LIKE '%MODEL%'
  BEGIN
    IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
    BEGIN--LONG-SHORT
      SELECT @MODEL_DEF_CD = SUBSTRING(@MODEL_PORTFOLIO_DEF_CD, 1, @MINUS_INDEX-1)
      UPDATE #RESULT SET ml_rtn = r.rtn
        FROM return_calc_params p, return_calc_result r
       WHERE #RESULT.begin_bdate = @BEGIN_BDATE
         AND #RESULT.end_bdate = @END_BDATE
         AND #RESULT.begin_bdate = p.bdate_from
         AND #RESULT.end_bdate = p.bdate_to
         AND p.return_type = 'MODEL'
         AND p.strategy_id = @STRATEGY_ID
         AND p.weight = @WEIGHT
         AND p.model_def_cd = @MODEL_DEF_CD
         AND p.return_calc_id = r.return_calc_id
         AND r.univ_type = 'TOTAL'
      SELECT @MODEL_DEF_CD = SUBSTRING(@MODEL_PORTFOLIO_DEF_CD, @MINUS_INDEX+1, LEN(@MODEL_PORTFOLIO_DEF_CD))
      UPDATE #RESULT SET ms_rtn = r.rtn
        FROM return_calc_params p, return_calc_result r
       WHERE #RESULT.begin_bdate = @BEGIN_BDATE
         AND #RESULT.end_bdate = @END_BDATE
         AND #RESULT.begin_bdate = p.bdate_from
         AND #RESULT.end_bdate = p.bdate_to
         AND p.return_type = 'MODEL'
         AND p.strategy_id = @STRATEGY_ID
         AND p.weight = @WEIGHT
         AND p.model_def_cd = @MODEL_DEF_CD
         AND p.return_calc_id = r.return_calc_id
         AND r.univ_type = 'TOTAL'
    END
    ELSE
    BEGIN--LONG ONLY
      UPDATE #RESULT SET ml_rtn = r.rtn
        FROM return_calc_params p, return_calc_result r
       WHERE #RESULT.begin_bdate = @BEGIN_BDATE
         AND #RESULT.end_bdate = @END_BDATE
         AND #RESULT.begin_bdate = p.bdate_from
         AND #RESULT.end_bdate = p.bdate_to
         AND p.return_type = 'MODEL'
         AND p.strategy_id = @STRATEGY_ID
         AND p.weight = @WEIGHT
         AND p.model_def_cd = @MODEL_PORTFOLIO_DEF_CD
         AND p.return_calc_id = r.return_calc_id
         AND r.univ_type = 'TOTAL'
    END
  END
END

IF @RETURN_TYPE = 'ACCT'
  BEGIN UPDATE #RESULT SET total_rtn = acct_rtn END
ELSE IF @RETURN_TYPE = 'BMK'
  BEGIN UPDATE #RESULT SET total_rtn = bmk_rtn END
ELSE IF @RETURN_TYPE = 'MODEL'
BEGIN
  IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
    BEGIN UPDATE #RESULT SET total_rtn = ml_rtn - ms_rtn END
  ELSE
    BEGIN UPDATE #RESULT SET total_rtn = ml_rtn END
END
ELSE IF @RETURN_TYPE = 'ACCT-BMK'
  BEGIN UPDATE #RESULT SET total_rtn = acct_rtn - bmk_rtn END
ELSE IF @RETURN_TYPE = 'MODEL-BMK'
BEGIN
  IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
    BEGIN UPDATE #RESULT SET total_rtn = ml_rtn - ms_rtn - bmk_rtn END
  ELSE
    BEGIN UPDATE #RESULT SET total_rtn = ml_rtn - bmk_rtn END
END
ELSE IF @RETURN_TYPE = 'ACCT-MODEL'
BEGIN
  IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
    BEGIN UPDATE #RESULT SET total_rtn = acct_rtn - ml_rtn - ms_rtn END
  ELSE
    BEGIN UPDATE #RESULT SET total_rtn = acct_rtn - ml_rtn END
END
/**** UPDATE TOTAL RETURN: END ****/

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (2)'
  SELECT * FROM #RESULT ORDER BY ordinal, end_bdate
END

/**** UPDATE UNIVERSE MODEL AND SECTOR MODEL RETURN: BEGIN ****/
IF @RETURN_TYPE LIKE '%MODEL%'
BEGIN
IF EXISTS (SELECT f.* FROM strategy g, factor_model_weights f
            WHERE g.strategy_id = @STRATEGY_ID
              AND g.factor_model_id = f.factor_model_id
              AND f.universe_total_wgt != 0.0)
  BEGIN
    UPDATE #RESULT SET ml_rtn=0.0, ms_rtn=0.0
    SELECT @PERIOD_ID = 0
    WHILE EXISTS (SELECT * FROM #RTN_PERIOD WHERE period_id > @PERIOD_ID)
    BEGIN
      SELECT @PERIOD_ID = MIN(period_id) FROM #RTN_PERIOD WHERE period_id > @PERIOD_ID
      SELECT @BEGIN_BDATE=begin_bdate, @END_BDATE=end_bdate FROM #RTN_PERIOD WHERE period_id=@PERIOD_ID
      IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
      BEGIN--LONG-SHORT
        SELECT @MODEL_DEF_CD = SUBSTRING(@MODEL_PORTFOLIO_DEF_CD, 1, @MINUS_INDEX-1)
        UPDATE #RESULT SET ml_rtn = r.rtn
          FROM return_calc_params p, return_calc_result r
         WHERE #RESULT.begin_bdate = @BEGIN_BDATE
           AND #RESULT.end_bdate = @END_BDATE
           AND #RESULT.begin_bdate = p.bdate_from
           AND #RESULT.end_bdate = p.bdate_to
           AND p.return_type = 'MODEL'
           AND p.strategy_id = @STRATEGY_ID
           AND p.weight = @WEIGHT
           AND p.model_def_cd = @MODEL_DEF_CD
           AND p.return_calc_id = r.return_calc_id
           AND r.univ_type = 'UNIVERSE'
        SELECT @MODEL_DEF_CD = SUBSTRING(@MODEL_PORTFOLIO_DEF_CD, @MINUS_INDEX+1, LEN(@MODEL_PORTFOLIO_DEF_CD))
        UPDATE #RESULT SET ms_rtn = r.rtn
          FROM return_calc_params p, return_calc_result r
         WHERE #RESULT.begin_bdate = @BEGIN_BDATE
           AND #RESULT.end_bdate = @END_BDATE
           AND #RESULT.begin_bdate = p.bdate_from
           AND #RESULT.end_bdate = p.bdate_to
           AND p.return_type = 'MODEL'
           AND p.strategy_id = @STRATEGY_ID
           AND p.weight = @WEIGHT
           AND p.model_def_cd = @MODEL_DEF_CD
           AND p.return_calc_id = r.return_calc_id
           AND r.univ_type = 'UNIVERSE'
      END
      ELSE
      BEGIN--LONG ONLY
        UPDATE #RESULT SET ml_rtn = r.rtn
          FROM return_calc_params p, return_calc_result r
         WHERE #RESULT.begin_bdate = @BEGIN_BDATE
           AND #RESULT.end_bdate = @END_BDATE
           AND #RESULT.begin_bdate = p.bdate_from
           AND #RESULT.end_bdate = p.bdate_to
           AND p.return_type = 'MODEL'
           AND p.strategy_id = @STRATEGY_ID
           AND p.weight = @WEIGHT
           AND p.model_def_cd = @MODEL_PORTFOLIO_DEF_CD
           AND p.return_calc_id = r.return_calc_id
           AND r.univ_type = 'UNIVERSE'
      END
    END

    IF @RETURN_TYPE = 'MODEL'
    BEGIN
      IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
        BEGIN UPDATE #RESULT SET universe_rtn = ml_rtn - ms_rtn END
      ELSE
        BEGIN UPDATE #RESULT SET universe_rtn = ml_rtn END
    END
    ELSE IF @RETURN_TYPE = 'MODEL-BMK'
    BEGIN
      IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
        BEGIN UPDATE #RESULT SET universe_rtn = ml_rtn - ms_rtn - bmk_rtn END
      ELSE
        BEGIN UPDATE #RESULT SET universe_rtn = ml_rtn - bmk_rtn END
    END
    ELSE IF @RETURN_TYPE = 'ACCT-MODEL'
    BEGIN
      IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
        BEGIN UPDATE #RESULT SET universe_rtn = acct_rtn - ml_rtn - ms_rtn END
      ELSE
        BEGIN UPDATE #RESULT SET universe_rtn = acct_rtn - ml_rtn END
    END
  END

IF EXISTS (SELECT f.* FROM strategy g, factor_model_weights f
            WHERE g.strategy_id = @STRATEGY_ID
              AND g.factor_model_id = f.factor_model_id
              AND f.sector_ss_wgt != 0.0
              AND f.ss_total_wgt != 0.0)
  BEGIN
    UPDATE #RESULT SET ml_rtn=0.0, ms_rtn=0.0
    SELECT @PERIOD_ID = 0
    WHILE EXISTS (SELECT * FROM #RTN_PERIOD WHERE period_id > @PERIOD_ID)
    BEGIN
      SELECT @PERIOD_ID = MIN(period_id) FROM #RTN_PERIOD WHERE period_id > @PERIOD_ID
      SELECT @BEGIN_BDATE=begin_bdate, @END_BDATE=end_bdate FROM #RTN_PERIOD WHERE period_id=@PERIOD_ID
      IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
      BEGIN--LONG-SHORT
        SELECT @MODEL_DEF_CD = SUBSTRING(@MODEL_PORTFOLIO_DEF_CD, 1, @MINUS_INDEX-1)
        UPDATE #RESULT SET ml_rtn = r.rtn
          FROM return_calc_params p, return_calc_result r
         WHERE #RESULT.begin_bdate = @BEGIN_BDATE
           AND #RESULT.end_bdate = @END_BDATE
           AND #RESULT.begin_bdate = p.bdate_from
           AND #RESULT.end_bdate = p.bdate_to
           AND p.return_type = 'MODEL'
           AND p.strategy_id = @STRATEGY_ID
           AND p.weight = @WEIGHT
           AND p.model_def_cd = @MODEL_DEF_CD
           AND p.return_calc_id = r.return_calc_id
           AND r.univ_type = 'SECTOR'
           AND r.sector_model_id IS NULL
        SELECT @MODEL_DEF_CD = SUBSTRING(@MODEL_PORTFOLIO_DEF_CD, @MINUS_INDEX+1, LEN(@MODEL_PORTFOLIO_DEF_CD))
        UPDATE #RESULT SET ms_rtn = r.rtn
          FROM return_calc_params p, return_calc_result r
         WHERE #RESULT.begin_bdate = @BEGIN_BDATE
           AND #RESULT.end_bdate = @END_BDATE
           AND #RESULT.begin_bdate = p.bdate_from
           AND #RESULT.end_bdate = p.bdate_to
           AND p.return_type = 'MODEL'
           AND p.strategy_id = @STRATEGY_ID
           AND p.weight = @WEIGHT
           AND p.model_def_cd = @MODEL_DEF_CD
           AND p.return_calc_id = r.return_calc_id
           AND r.univ_type = 'SECTOR'
           AND r.sector_model_id IS NULL
      END
      ELSE
      BEGIN--LONG ONLY
        UPDATE #RESULT SET ml_rtn = r.rtn
          FROM return_calc_params p, return_calc_result r
         WHERE #RESULT.begin_bdate = @BEGIN_BDATE
           AND #RESULT.end_bdate = @END_BDATE
           AND #RESULT.begin_bdate = p.bdate_from
           AND #RESULT.end_bdate = p.bdate_to
           AND p.return_type = 'MODEL'
           AND p.strategy_id = @STRATEGY_ID
           AND p.weight = @WEIGHT
           AND p.model_def_cd = @MODEL_PORTFOLIO_DEF_CD
           AND p.return_calc_id = r.return_calc_id
           AND r.univ_type = 'SECTOR'
           AND r.sector_model_id IS NULL
      END
    END

    IF @RETURN_TYPE = 'MODEL'
    BEGIN
      IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
        BEGIN UPDATE #RESULT SET sector_rtn = ml_rtn - ms_rtn END
      ELSE
        BEGIN UPDATE #RESULT SET sector_rtn = ml_rtn END
    END
    ELSE IF @RETURN_TYPE = 'MODEL-BMK'
    BEGIN
      IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
        BEGIN UPDATE #RESULT SET sector_rtn = ml_rtn - ms_rtn - bmk_rtn END
      ELSE
        BEGIN UPDATE #RESULT SET sector_rtn = ml_rtn - bmk_rtn END
    END
    ELSE IF @RETURN_TYPE = 'ACCT-MODEL'
    BEGIN
      IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
        BEGIN UPDATE #RESULT SET sector_rtn = acct_rtn - ml_rtn - ms_rtn END
      ELSE
        BEGIN UPDATE #RESULT SET sector_rtn = acct_rtn - ml_rtn END
    END
  END
END
/**** UPDATE UNIVERSE MODEL AND SECTOR MODEL RETURN: END ****/

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (3)'
  SELECT * FROM #RESULT ORDER BY ordinal, end_bdate
END

DECLARE @SQL varchar(1500),
        @SECTOR_MODEL_ID int,
        @SECTOR_ID int,
        @SECTOR_NUM int,
        @UNKNOWN_SECTOR bit

SELECT @SECTOR_MODEL_ID = f.sector_model_id
  FROM strategy g, factor_model f
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = f.factor_model_id

/**** UPDATE INDIVIDUAL SECTOR RETURN: BEGIN ****/
SELECT @SECTOR_NUM = 0
WHILE EXISTS (SELECT * FROM sector_def WHERE sector_model_id = @SECTOR_MODEL_ID AND sector_num > @SECTOR_NUM)
BEGIN
  SELECT @SECTOR_NUM = MIN(sector_num) FROM sector_def
   WHERE sector_model_id = @SECTOR_MODEL_ID
     AND sector_num > @SECTOR_NUM

  SELECT @SECTOR_ID = sector_id FROM sector_def
   WHERE sector_model_id = @SECTOR_MODEL_ID
     AND sector_num = @SECTOR_NUM

  UPDATE #RESULT SET acct_rtn=0.0, bmk_rtn=0.0, ml_rtn=0.0, ms_rtn=0.0
  SELECT @PERIOD_ID = 0
  WHILE EXISTS (SELECT * FROM #RTN_PERIOD WHERE period_id > @PERIOD_ID)
  BEGIN
    SELECT @PERIOD_ID = MIN(period_id) FROM #RTN_PERIOD WHERE period_id > @PERIOD_ID
    SELECT @BEGIN_BDATE=begin_bdate, @END_BDATE=end_bdate FROM #RTN_PERIOD WHERE period_id=@PERIOD_ID
    IF @RETURN_TYPE LIKE '%ACCT%'
    BEGIN
      UPDATE #RESULT SET acct_rtn = r.rtn
        FROM return_calc_params p, return_calc_result r
       WHERE #RESULT.begin_bdate = @BEGIN_BDATE
         AND #RESULT.end_bdate = @END_BDATE
         AND #RESULT.begin_bdate = p.bdate_from
         AND #RESULT.end_bdate = p.bdate_to
         AND p.return_type = 'ACCT'
         AND p.strategy_id = @STRATEGY_ID
         AND p.weight = 'MVAL'
         AND p.account_cd = @ACCOUNT_CD
         AND p.return_calc_id = r.return_calc_id
         AND r.univ_type = 'SECTOR'
         AND r.sector_model_id = @SECTOR_MODEL_ID
         AND r.sector_id = @SECTOR_ID
    END
    IF @RETURN_TYPE LIKE '%BMK%'
    BEGIN
      UPDATE #RESULT SET bmk_rtn = r.rtn
        FROM return_calc_params p, return_calc_result r
       WHERE #RESULT.begin_bdate = @BEGIN_BDATE
         AND #RESULT.end_bdate = @END_BDATE
         AND #RESULT.begin_bdate = p.bdate_from
         AND #RESULT.end_bdate = p.bdate_to
         AND p.return_type = 'BMK'
         AND p.strategy_id = @STRATEGY_ID
         AND p.weight = @WEIGHT
         AND p.bm_universe_id = @BM_UNIVERSE_ID
         AND p.return_calc_id = r.return_calc_id
         AND r.univ_type = 'SECTOR'
         AND r.sector_model_id = @SECTOR_MODEL_ID
         AND r.sector_id = @SECTOR_ID
    END
    IF @RETURN_TYPE LIKE '%MODEL%'
    BEGIN
      IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
      BEGIN--LONG-SHORT
        SELECT @MODEL_DEF_CD = SUBSTRING(@MODEL_PORTFOLIO_DEF_CD, 1, @MINUS_INDEX-1)
        UPDATE #RESULT SET ml_rtn = r.rtn
          FROM return_calc_params p, return_calc_result r
         WHERE #RESULT.begin_bdate = @BEGIN_BDATE
           AND #RESULT.end_bdate = @END_BDATE
           AND #RESULT.begin_bdate = p.bdate_from
           AND #RESULT.end_bdate = p.bdate_to
           AND p.return_type = 'MODEL'
           AND p.strategy_id = @STRATEGY_ID
           AND p.weight = @WEIGHT
           AND p.model_def_cd = @MODEL_DEF_CD
           AND p.return_calc_id = r.return_calc_id
           AND r.univ_type = 'SECTOR'
           AND r.sector_model_id = @SECTOR_MODEL_ID
           AND r.sector_id = @SECTOR_ID
        SELECT @MODEL_DEF_CD = SUBSTRING(@MODEL_PORTFOLIO_DEF_CD, @MINUS_INDEX+1, LEN(@MODEL_PORTFOLIO_DEF_CD))
        UPDATE #RESULT SET ms_rtn = r.rtn
          FROM return_calc_params p, return_calc_result r
         WHERE #RESULT.begin_bdate = @BEGIN_BDATE
           AND #RESULT.end_bdate = @END_BDATE
           AND #RESULT.begin_bdate = p.bdate_from
           AND #RESULT.end_bdate = p.bdate_to
           AND p.return_type = 'MODEL'
           AND p.strategy_id = @STRATEGY_ID
           AND p.weight = @WEIGHT
           AND p.model_def_cd = @MODEL_DEF_CD
           AND p.return_calc_id = r.return_calc_id
           AND r.univ_type = 'SECTOR'
           AND r.sector_model_id = @SECTOR_MODEL_ID
           AND r.sector_id = @SECTOR_ID
      END
      ELSE
      BEGIN--LONG ONLY
        UPDATE #RESULT SET ml_rtn = r.rtn
          FROM return_calc_params p, return_calc_result r
         WHERE #RESULT.begin_bdate = @BEGIN_BDATE
           AND #RESULT.end_bdate = @END_BDATE
           AND #RESULT.begin_bdate = p.bdate_from
           AND #RESULT.end_bdate = p.bdate_to
           AND p.return_type = 'MODEL'
           AND p.strategy_id = @STRATEGY_ID
           AND p.weight = @WEIGHT
           AND p.model_def_cd = @MODEL_PORTFOLIO_DEF_CD
           AND p.return_calc_id = r.return_calc_id
           AND r.univ_type = 'SECTOR'
           AND r.sector_model_id = @SECTOR_MODEL_ID
           AND r.sector_id = @SECTOR_ID
      END
    END
  END

  SELECT @SQL = 'ALTER TABLE #RESULT ADD sector'+CONVERT(varchar,@SECTOR_NUM)+' float NULL'
  EXEC(@SQL)

  IF @RETURN_TYPE = 'ACCT'
    BEGIN SELECT @SQL = 'UPDATE #RESULT SET sector'+CONVERT(varchar,@SECTOR_NUM)+' = acct_rtn' EXEC(@SQL) END
  ELSE IF @RETURN_TYPE = 'BMK'
    BEGIN SELECT @SQL = 'UPDATE #RESULT SET sector'+CONVERT(varchar,@SECTOR_NUM)+' = bmk_rtn' EXEC(@SQL) END
  ELSE IF @RETURN_TYPE = 'MODEL'
  BEGIN
    IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
      BEGIN SELECT @SQL = 'UPDATE #RESULT SET sector'+CONVERT(varchar,@SECTOR_NUM)+' = ml_rtn - ms_rtn' EXEC(@SQL) END
    ELSE
      BEGIN SELECT @SQL = 'UPDATE #RESULT SET sector'+CONVERT(varchar,@SECTOR_NUM)+' = ml_rtn' EXEC(@SQL) END
  END
  ELSE IF @RETURN_TYPE = 'ACCT-BMK'
    BEGIN SELECT @SQL = 'UPDATE #RESULT SET sector'+CONVERT(varchar,@SECTOR_NUM)+' = acct_rtn - bmk_rtn' EXEC(@SQL) END
  ELSE IF @RETURN_TYPE = 'MODEL-BMK'
  BEGIN
    IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
      BEGIN SELECT @SQL = 'UPDATE #RESULT SET sector'+CONVERT(varchar,@SECTOR_NUM)+' = ml_rtn - ms_rtn - bmk_rtn' EXEC(@SQL) END
    ELSE
      BEGIN SELECT @SQL = 'UPDATE #RESULT SET sector'+CONVERT(varchar,@SECTOR_NUM)+' = ml_rtn - bmk_rtn' EXEC(@SQL) END
  END
  ELSE IF @RETURN_TYPE = 'ACCT-MODEL'
  BEGIN
    IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
      BEGIN SELECT @SQL = 'UPDATE #RESULT SET sector'+CONVERT(varchar,@SECTOR_NUM)+' = acct_rtn - ml_rtn - ms_rtn' EXEC(@SQL) END
    ELSE
      BEGIN SELECT @SQL = 'UPDATE #RESULT SET sector'+CONVERT(varchar,@SECTOR_NUM)+' = acct_rtn - ml_rtn' EXEC(@SQL) END
  END
END

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (4)'
  SELECT * FROM #RESULT ORDER BY ordinal, end_bdate
END

/**** CHECK FOR UNKNOWN SECTOR: BEGIN ****/
SELECT @UNKNOWN_SECTOR = 0
SELECT @PERIOD_ID = 0
WHILE EXISTS (SELECT * FROM #RTN_PERIOD WHERE period_id > @PERIOD_ID)
BEGIN
  SELECT @PERIOD_ID = MIN(period_id) FROM #RTN_PERIOD WHERE period_id > @PERIOD_ID
  SELECT @BEGIN_BDATE=begin_bdate, @END_BDATE=end_bdate FROM #RTN_PERIOD WHERE period_id=@PERIOD_ID
  IF @RETURN_TYPE LIKE '%ACCT%'
  BEGIN
    IF EXISTS (SELECT r.* FROM return_calc_params p, return_calc_result r
                WHERE p.bdate_from = @BEGIN_BDATE
                  AND p.bdate_to = @END_BDATE
                  AND p.return_type = 'ACCT'
                  AND p.strategy_id = @STRATEGY_ID
                  AND p.weight = 'MVAL'
                  AND p.account_cd = @ACCOUNT_CD
                  AND p.return_calc_id = r.return_calc_id
                  AND r.univ_type = 'SECTOR'
                  AND r.sector_model_id = @SECTOR_MODEL_ID
                  AND r.sector_id IS NULL
                  AND r.segment_id IS NULL)
      BEGIN SELECT @UNKNOWN_SECTOR = 1 END
  END
  IF @RETURN_TYPE LIKE '%BMK%'
  BEGIN
    IF EXISTS (SELECT r.* FROM return_calc_params p, return_calc_result r
                WHERE p.bdate_from = @BEGIN_BDATE
                  AND p.bdate_to = @END_BDATE
                  AND p.return_type = 'BMK'
                  AND p.strategy_id = @STRATEGY_ID
                  AND p.weight = @WEIGHT
                  AND p.bm_universe_id = @BM_UNIVERSE_ID
                  AND p.return_calc_id = r.return_calc_id
                  AND r.univ_type = 'SECTOR'
                  AND r.sector_model_id = @SECTOR_MODEL_ID
                  AND r.sector_id IS NULL
                  AND r.segment_id IS NULL)
      BEGIN SELECT @UNKNOWN_SECTOR = 1 END
  END
  IF @RETURN_TYPE LIKE '%MODEL%'
  BEGIN
    IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
    BEGIN--LONG-SHORT
      SELECT @MODEL_DEF_CD = SUBSTRING(@MODEL_PORTFOLIO_DEF_CD, 1, @MINUS_INDEX-1)
      IF EXISTS (SELECT r.* FROM return_calc_params p, return_calc_result r
                  WHERE p.bdate_from = @BEGIN_BDATE
                    AND p.bdate_to = @END_BDATE
                    AND p.return_type = 'MODEL'
                    AND p.strategy_id = @STRATEGY_ID
                    AND p.weight = @WEIGHT
                    AND p.model_def_cd = @MODEL_DEF_CD
                    AND p.return_calc_id = r.return_calc_id
                    AND r.univ_type = 'SECTOR'
                    AND r.sector_model_id = @SECTOR_MODEL_ID
                    AND r.sector_id IS NULL
                    AND r.segment_id IS NULL)
        BEGIN SELECT @UNKNOWN_SECTOR = 1 END
      SELECT @MODEL_DEF_CD = SUBSTRING(@MODEL_PORTFOLIO_DEF_CD, @MINUS_INDEX+1, LEN(@MODEL_PORTFOLIO_DEF_CD))
      IF EXISTS (SELECT r.* FROM return_calc_params p, return_calc_result r
                  WHERE p.bdate_from = @BEGIN_BDATE
                    AND p.bdate_to = @END_BDATE
                    AND p.return_type = 'MODEL'
                    AND p.strategy_id = @STRATEGY_ID
                    AND p.weight = @WEIGHT
                    AND p.model_def_cd = @MODEL_DEF_CD
                    AND p.return_calc_id = r.return_calc_id
                    AND r.univ_type = 'SECTOR'
                    AND r.sector_model_id = @SECTOR_MODEL_ID
                    AND r.sector_id IS NULL
                    AND r.segment_id IS NULL)
        BEGIN SELECT @UNKNOWN_SECTOR = 1 END
    END
    ELSE
    BEGIN--LONG ONLY
      IF EXISTS (SELECT r.* FROM return_calc_params p, return_calc_result r
                  WHERE p.bdate_from = @BEGIN_BDATE
                    AND p.bdate_to = @END_BDATE
                    AND p.return_type = 'MODEL'
                    AND p.strategy_id = @STRATEGY_ID
                    AND p.weight = @WEIGHT
                    AND p.model_def_cd = @MODEL_PORTFOLIO_DEF_CD
                    AND p.return_calc_id = r.return_calc_id
                    AND r.univ_type = 'SECTOR'
                    AND r.sector_model_id = @SECTOR_MODEL_ID
                    AND r.sector_id IS NULL
                    AND r.segment_id IS NULL)
        BEGIN SELECT @UNKNOWN_SECTOR = 1 END
    END
  END
END
/**** CHECK FOR UNKNOWN SECTOR: END ****/

/**** ADD UNKNOWN SECTOR: BEGIN ****/
IF @UNKNOWN_SECTOR = 1
BEGIN
  UPDATE #RESULT SET acct_rtn=0.0, bmk_rtn=0.0, ml_rtn=0.0, ms_rtn=0.0
  SELECT @PERIOD_ID = 0
  WHILE EXISTS (SELECT * FROM #RTN_PERIOD WHERE period_id > @PERIOD_ID)
  BEGIN
    SELECT @PERIOD_ID = MIN(period_id) FROM #RTN_PERIOD WHERE period_id > @PERIOD_ID
    SELECT @BEGIN_BDATE=begin_bdate, @END_BDATE=end_bdate FROM #RTN_PERIOD WHERE period_id=@PERIOD_ID
    IF @RETURN_TYPE LIKE '%ACCT%'
    BEGIN
      UPDATE #RESULT SET acct_rtn = r.rtn
        FROM return_calc_params p, return_calc_result r
       WHERE #RESULT.begin_bdate = @BEGIN_BDATE
         AND #RESULT.end_bdate = @END_BDATE
         AND #RESULT.begin_bdate = p.bdate_from
         AND #RESULT.end_bdate = p.bdate_to
         AND p.return_type = 'ACCT'
         AND p.strategy_id = @STRATEGY_ID
         AND p.weight = 'MVAL'
         AND p.account_cd = @ACCOUNT_CD
         AND p.return_calc_id = r.return_calc_id
         AND r.univ_type = 'SECTOR'
         AND r.sector_model_id = @SECTOR_MODEL_ID
         AND r.sector_id IS NULL
         AND r.segment_id IS NULL
    END
    IF @RETURN_TYPE LIKE '%BMK%'
    BEGIN
      UPDATE #RESULT SET bmk_rtn = r.rtn
        FROM return_calc_params p, return_calc_result r
       WHERE #RESULT.begin_bdate = @BEGIN_BDATE
         AND #RESULT.end_bdate = @END_BDATE
         AND #RESULT.begin_bdate = p.bdate_from
         AND #RESULT.end_bdate = p.bdate_to
         AND p.return_type = 'BMK'
         AND p.strategy_id = @STRATEGY_ID
         AND p.weight = @WEIGHT
         AND p.bm_universe_id = @BM_UNIVERSE_ID
         AND p.return_calc_id = r.return_calc_id
         AND r.univ_type = 'SECTOR'
         AND r.sector_model_id = @SECTOR_MODEL_ID
         AND r.sector_id IS NULL
         AND r.segment_id IS NULL
    END
    IF @RETURN_TYPE LIKE '%MODEL%'
    BEGIN
      IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
      BEGIN--LONG-SHORT
        SELECT @MODEL_DEF_CD = SUBSTRING(@MODEL_PORTFOLIO_DEF_CD, 1, @MINUS_INDEX-1)
        UPDATE #RESULT SET ml_rtn = r.rtn
          FROM return_calc_params p, return_calc_result r
         WHERE #RESULT.begin_bdate = @BEGIN_BDATE
           AND #RESULT.end_bdate = @END_BDATE
           AND #RESULT.begin_bdate = p.bdate_from
           AND #RESULT.end_bdate = p.bdate_to
           AND p.return_type = 'MODEL'
           AND p.strategy_id = @STRATEGY_ID
           AND p.weight = @WEIGHT
           AND p.model_def_cd = @MODEL_DEF_CD
           AND p.return_calc_id = r.return_calc_id
           AND r.univ_type = 'SECTOR'
           AND r.sector_model_id = @SECTOR_MODEL_ID
           AND r.sector_id IS NULL
           AND r.segment_id IS NULL
        SELECT @MODEL_DEF_CD = SUBSTRING(@MODEL_PORTFOLIO_DEF_CD, @MINUS_INDEX+1, LEN(@MODEL_PORTFOLIO_DEF_CD))
        UPDATE #RESULT SET ms_rtn = r.rtn
          FROM return_calc_params p, return_calc_result r
         WHERE #RESULT.begin_bdate = @BEGIN_BDATE
           AND #RESULT.end_bdate = @END_BDATE
           AND #RESULT.begin_bdate = p.bdate_from
           AND #RESULT.end_bdate = p.bdate_to
           AND p.return_type = 'MODEL'
           AND p.strategy_id = @STRATEGY_ID
           AND p.weight = @WEIGHT
           AND p.model_def_cd = @MODEL_DEF_CD
           AND p.return_calc_id = r.return_calc_id
           AND r.univ_type = 'SECTOR'
           AND r.sector_model_id = @SECTOR_MODEL_ID
           AND r.sector_id IS NULL
           AND r.segment_id IS NULL
      END
      ELSE
      BEGIN--LONG ONLY
        UPDATE #RESULT SET ml_rtn = r.rtn
          FROM return_calc_params p, return_calc_result r
         WHERE #RESULT.begin_bdate = @BEGIN_BDATE
           AND #RESULT.end_bdate = @END_BDATE
           AND #RESULT.begin_bdate = p.bdate_from
           AND #RESULT.end_bdate = p.bdate_to
           AND p.return_type = 'MODEL'
           AND p.strategy_id = @STRATEGY_ID
           AND p.weight = @WEIGHT
           AND p.model_def_cd = @MODEL_PORTFOLIO_DEF_CD
           AND p.return_calc_id = r.return_calc_id
           AND r.univ_type = 'SECTOR'
           AND r.sector_model_id = @SECTOR_MODEL_ID
           AND r.sector_id IS NULL
           AND r.segment_id IS NULL
      END
    END
  END

  SELECT @SECTOR_NUM = @SECTOR_NUM + 1
  SELECT @SQL = 'ALTER TABLE #RESULT ADD sector'+CONVERT(varchar,@SECTOR_NUM)+' float NULL'
  EXEC(@SQL)

  IF @RETURN_TYPE = 'ACCT'
    BEGIN SELECT @SQL = 'UPDATE #RESULT SET sector'+CONVERT(varchar,@SECTOR_NUM)+' = acct_rtn' EXEC(@SQL) END
  ELSE IF @RETURN_TYPE = 'BMK'
    BEGIN SELECT @SQL = 'UPDATE #RESULT SET sector'+CONVERT(varchar,@SECTOR_NUM)+' = bmk_rtn' EXEC(@SQL) END
  ELSE IF @RETURN_TYPE = 'MODEL'
  BEGIN
    IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
      BEGIN SELECT @SQL = 'UPDATE #RESULT SET sector'+CONVERT(varchar,@SECTOR_NUM)+' = ml_rtn - ms_rtn' EXEC(@SQL) END
    ELSE
      BEGIN SELECT @SQL = 'UPDATE #RESULT SET sector'+CONVERT(varchar,@SECTOR_NUM)+' = ml_rtn' EXEC(@SQL) END
  END
  ELSE IF @RETURN_TYPE = 'ACCT-BMK'
    BEGIN SELECT @SQL = 'UPDATE #RESULT SET sector'+CONVERT(varchar,@SECTOR_NUM)+' = acct_rtn - bmk_rtn' EXEC(@SQL) END
  ELSE IF @RETURN_TYPE = 'MODEL-BMK'
  BEGIN
    IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
      BEGIN SELECT @SQL = 'UPDATE #RESULT SET sector'+CONVERT(varchar,@SECTOR_NUM)+' = ml_rtn - ms_rtn - bmk_rtn' EXEC(@SQL) END
    ELSE
      BEGIN SELECT @SQL = 'UPDATE #RESULT SET sector'+CONVERT(varchar,@SECTOR_NUM)+' = ml_rtn - bmk_rtn' EXEC(@SQL) END
  END
  ELSE IF @RETURN_TYPE = 'ACCT-MODEL'
  BEGIN
    IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
      BEGIN SELECT @SQL = 'UPDATE #RESULT SET sector'+CONVERT(varchar,@SECTOR_NUM)+' = acct_rtn - ml_rtn - ms_rtn' EXEC(@SQL) END
    ELSE
      BEGIN SELECT @SQL = 'UPDATE #RESULT SET sector'+CONVERT(varchar,@SECTOR_NUM)+' = acct_rtn - ml_rtn' EXEC(@SQL) END
  END
END
/**** ADD UNKNOWN SECTOR: END ****/
/**** UPDATE INDIVIDUAL SECTOR RETURN: END ****/

DROP TABLE #RTN_PERIOD

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (5)'
  SELECT * FROM #RESULT ORDER BY ordinal, end_bdate
END

DECLARE @SECTOR_NM varchar(64),
        @MAX_SECTOR_NUM int

SELECT @MAX_SECTOR_NUM = @SECTOR_NUM
SELECT @SQL = 'SELECT period AS [Period], total_rtn AS [Total], universe_rtn AS [Universe], sector_rtn AS [Sector]'

SELECT @SECTOR_NUM = 1
WHILE @SECTOR_NUM <= @MAX_SECTOR_NUM
BEGIN
  IF EXISTS (SELECT * FROM sector_def WHERE sector_model_id = @SECTOR_MODEL_ID AND sector_num = @SECTOR_NUM)
    BEGIN SELECT @SECTOR_NM = sector_nm FROM sector_def WHERE sector_model_id = @SECTOR_MODEL_ID AND sector_num = @SECTOR_NUM END
  ELSE
    BEGIN SELECT @SECTOR_NM = 'UNKNOWN' END

  SELECT @SQL = @SQL + ', sector'+CONVERT(varchar,@SECTOR_NUM)+' AS ['+@SECTOR_NM+']'
  SELECT @SECTOR_NUM = @SECTOR_NUM + 1
END

IF @RETURN_TYPE = 'ACCT'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NOT NULL AND begin_bdate >= (SELECT MIN(bdate) FROM position WHERE account_cd = '''+@ACCOUNT_CD+''') ORDER BY ordinal' END
ELSE IF @RETURN_TYPE = 'BMK'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NOT NULL AND begin_bdate >= (SELECT MIN(universe_dt) FROM universe_makeup WHERE universe_id = '+CONVERT(varchar,@BM_UNIVERSE_ID)+') ORDER BY ordinal' END
ELSE IF @RETURN_TYPE = 'MODEL'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NOT NULL AND begin_bdate >= (SELECT MIN(bdate) FROM scores WHERE strategy_id = '+CONVERT(varchar,@STRATEGY_ID)+') ORDER BY ordinal' END
ELSE IF @RETURN_TYPE = 'ACCT-BMK'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NOT NULL AND begin_bdate >= (SELECT MAX(bdate) FROM (SELECT MIN(bdate) AS [bdate] FROM position WHERE account_cd = '''+@ACCOUNT_CD+''' UNION SELECT MIN(universe_dt) AS [bdate] FROM universe_makeup WHERE universe_id = '+CONVERT(varchar,@BM_UNIVERSE_ID)+') x) ORDER BY ordinal' END
ELSE IF @RETURN_TYPE = 'MODEL-BMK'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NOT NULL AND begin_bdate >= (SELECT MAX(bdate) FROM (SELECT MIN(bdate) AS [bdate] FROM scores WHERE strategy_id = '+CONVERT(varchar,@STRATEGY_ID)+' UNION SELECT MIN(universe_dt) AS [bdate] FROM universe_makeup WHERE universe_id = '+CONVERT(varchar,@BM_UNIVERSE_ID)+') x) ORDER BY ordinal' END
ELSE IF @RETURN_TYPE = 'ACCT-MODEL'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NOT NULL AND begin_bdate >= (SELECT MAX(bdate) FROM (SELECT MIN(bdate) AS [bdate] FROM position WHERE account_cd = '''+@ACCOUNT_CD+''' UNION SELECT MIN(bdate) AS [bdate] FROM scores WHERE strategy_id = '+CONVERT(varchar,@STRATEGY_ID)+') x) ORDER BY ordinal' END
/*
IF @RETURN_TYPE = 'ACCT'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NOT NULL AND begin_bdate > (SELECT MIN(bdate) FROM position WHERE account_cd = '''+@ACCOUNT_CD+''') ORDER BY ordinal' END
ELSE IF @RETURN_TYPE = 'BMK'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NOT NULL AND begin_bdate > (SELECT MIN(universe_dt) FROM universe_makeup WHERE universe_id = '+CONVERT(varchar,@BM_UNIVERSE_ID)+') ORDER BY ordinal' END
ELSE IF @RETURN_TYPE = 'MODEL'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NOT NULL AND begin_bdate > (SELECT MIN(bdate) FROM scores WHERE strategy_id = '+CONVERT(varchar,@STRATEGY_ID)+') ORDER BY ordinal' END
ELSE IF @RETURN_TYPE = 'ACCT-BMK'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NOT NULL AND begin_bdate > (SELECT MAX(bdate) FROM (SELECT MIN(bdate) AS [bdate] FROM position WHERE account_cd = '''+@ACCOUNT_CD+''' UNION SELECT MIN(universe_dt) AS [bdate] FROM universe_makeup WHERE universe_id = '+CONVERT(varchar,@BM_UNIVERSE_ID)+') x) ORDER BY ordinal' END
ELSE IF @RETURN_TYPE = 'MODEL-BMK'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NOT NULL AND begin_bdate > (SELECT MAX(bdate) FROM (SELECT MIN(bdate) AS [bdate] FROM scores WHERE strategy_id = '+CONVERT(varchar,@STRATEGY_ID)+' UNION SELECT MIN(universe_dt) AS [bdate] FROM universe_makeup WHERE universe_id = '+CONVERT(varchar,@BM_UNIVERSE_ID)+') x) ORDER BY ordinal' END
ELSE IF @RETURN_TYPE = 'ACCT-MODEL'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NOT NULL AND begin_bdate > (SELECT MAX(bdate) FROM (SELECT MIN(bdate) AS [bdate] FROM position WHERE account_cd = '''+@ACCOUNT_CD+''' UNION SELECT MIN(bdate) AS [bdate] FROM scores WHERE strategy_id = '+CONVERT(varchar,@STRATEGY_ID)+') x) ORDER BY ordinal' END
*/
IF @DEBUG = 1
  BEGIN SELECT '@SQL', @SQL END

EXEC(@SQL)

SELECT @SQL = 'SELECT begin_bdate AS [From], end_bdate AS [To], total_rtn AS [Total], universe_rtn AS [Universe], sector_rtn AS [Sector]'

SELECT @SECTOR_NUM = 1
WHILE @SECTOR_NUM <= @MAX_SECTOR_NUM
BEGIN
  IF EXISTS (SELECT * FROM sector_def WHERE sector_model_id = @SECTOR_MODEL_ID AND sector_num = @SECTOR_NUM)
    BEGIN SELECT @SECTOR_NM = sector_nm FROM sector_def WHERE sector_model_id = @SECTOR_MODEL_ID AND sector_num = @SECTOR_NUM END
  ELSE
    BEGIN SELECT @SECTOR_NM = 'UNKNOWN' END

  SELECT @SQL = @SQL + ', sector'+CONVERT(varchar,@SECTOR_NUM)+' AS ['+@SECTOR_NM+']'
  SELECT @SECTOR_NUM = @SECTOR_NUM + 1
END

IF @RETURN_TYPE = 'ACCT'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NULL AND begin_bdate >= ''20070101'' AND begin_bdate >= (SELECT MIN(bdate) FROM position WHERE account_cd = '''+@ACCOUNT_CD+''') ORDER BY end_bdate DESC' END
ELSE IF @RETURN_TYPE = 'BMK'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NULL AND begin_bdate >= ''20070101'' AND begin_bdate >= (SELECT MIN(universe_dt) FROM universe_makeup WHERE universe_id = '+CONVERT(varchar,@BM_UNIVERSE_ID)+') ORDER BY end_bdate DESC' END
ELSE IF @RETURN_TYPE = 'MODEL'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NULL AND begin_bdate >= ''20070101'' AND begin_bdate >= (SELECT MIN(bdate) FROM scores WHERE strategy_id = '+CONVERT(varchar,@STRATEGY_ID)+') ORDER BY end_bdate DESC' END
ELSE IF @RETURN_TYPE = 'ACCT-BMK'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NULL AND begin_bdate >= ''20070101'' AND begin_bdate >= (SELECT MAX(bdate) FROM (SELECT MIN(bdate) AS [bdate] FROM position WHERE account_cd = '''+@ACCOUNT_CD+''' UNION SELECT MIN(universe_dt) AS [bdate] FROM universe_makeup WHERE universe_id = '+CONVERT(varchar,@BM_UNIVERSE_ID)+') x) ORDER BY end_bdate DESC' END
ELSE IF @RETURN_TYPE = 'MODEL-BMK'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NULL AND begin_bdate >= ''20070101'' AND begin_bdate >= (SELECT MAX(bdate) FROM (SELECT MIN(bdate) AS [bdate] FROM scores WHERE strategy_id = '+CONVERT(varchar,@STRATEGY_ID)+' UNION SELECT MIN(universe_dt) AS [bdate] FROM universe_makeup WHERE universe_id = '+CONVERT(varchar,@BM_UNIVERSE_ID)+') x) ORDER BY end_bdate DESC' END
ELSE IF @RETURN_TYPE = 'ACCT-MODEL'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NULL AND begin_bdate >= ''20070101'' AND begin_bdate >= (SELECT MAX(bdate) FROM (SELECT MIN(bdate) AS [bdate] FROM position WHERE account_cd = '''+@ACCOUNT_CD+''' UNION SELECT MIN(bdate) AS [bdate] FROM scores WHERE strategy_id = '+CONVERT(varchar,@STRATEGY_ID)+') x) ORDER BY end_bdate DESC' END
/*
IF @RETURN_TYPE = 'ACCT'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NULL AND begin_bdate >= ''20070101'' AND begin_bdate > (SELECT MIN(bdate) FROM position WHERE account_cd = '''+@ACCOUNT_CD+''') ORDER BY end_bdate DESC' END
ELSE IF @RETURN_TYPE = 'BMK'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NULL AND begin_bdate >= ''20070101'' AND begin_bdate > (SELECT MIN(universe_dt) FROM universe_makeup WHERE universe_id = '+CONVERT(varchar,@BM_UNIVERSE_ID)+') ORDER BY end_bdate DESC' END
ELSE IF @RETURN_TYPE = 'MODEL'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NULL AND begin_bdate >= ''20070101'' AND begin_bdate > (SELECT MIN(bdate) FROM scores WHERE strategy_id = '+CONVERT(varchar,@STRATEGY_ID)+') ORDER BY end_bdate DESC' END
ELSE IF @RETURN_TYPE = 'ACCT-BMK'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NULL AND begin_bdate >= ''20070101'' AND begin_bdate > (SELECT MAX(bdate) FROM (SELECT MIN(bdate) AS [bdate] FROM position WHERE account_cd = '''+@ACCOUNT_CD+''' UNION SELECT MIN(universe_dt) AS [bdate] FROM universe_makeup WHERE universe_id = '+CONVERT(varchar,@BM_UNIVERSE_ID)+') x) ORDER BY end_bdate DESC' END
ELSE IF @RETURN_TYPE = 'MODEL-BMK'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NULL AND begin_bdate >= ''20070101'' AND begin_bdate > (SELECT MAX(bdate) FROM (SELECT MIN(bdate) AS [bdate] FROM scores WHERE strategy_id = '+CONVERT(varchar,@STRATEGY_ID)+' UNION SELECT MIN(universe_dt) AS [bdate] FROM universe_makeup WHERE universe_id = '+CONVERT(varchar,@BM_UNIVERSE_ID)+') x) ORDER BY end_bdate DESC' END
ELSE IF @RETURN_TYPE = 'ACCT-MODEL'
  BEGIN SELECT @SQL = @SQL + ' FROM #RESULT WHERE ordinal IS NULL AND begin_bdate >= ''20070101'' AND begin_bdate > (SELECT MAX(bdate) FROM (SELECT MIN(bdate) AS [bdate] FROM position WHERE account_cd = '''+@ACCOUNT_CD+''' UNION SELECT MIN(bdate) AS [bdate] FROM scores WHERE strategy_id = '+CONVERT(varchar,@STRATEGY_ID)+') x) ORDER BY end_bdate DESC' END
*/
IF @DEBUG = 1
  BEGIN SELECT '@SQL', @SQL END

EXEC(@SQL)

DROP TABLE #RESULT

RETURN 0
go
IF OBJECT_ID('dbo.rpt_model_performance') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_model_performance >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_model_performance >>>'
go
