use QER
go
IF OBJECT_ID('dbo.return_calculate') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.return_calculate
    IF OBJECT_ID('dbo.return_calculate') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.return_calculate >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.return_calculate >>>'
END
go
CREATE PROCEDURE dbo.return_calculate @BDATE_FROM datetime,
                                      @BDATE_TO datetime,
                                      @RETURN_TYPE varchar(16),
                                      @STRATEGY_ID int,
                                      @WEIGHT varchar(16) = NULL,
                                      @ACCOUNT_CD varchar(32) = NULL,
                                      @BENCHMARK_CD varchar(50) = NULL,
                                      @MODEL_DEF_CD varchar(32) = NULL,
                                      @DEBUG bit = NULL
AS

IF @BDATE_FROM IS NULL
  BEGIN SELECT 'ERROR: @BDATE_FROM IS A REQUIRED PARAMETER' RETURN -1 END
IF @BDATE_TO IS NULL
  BEGIN SELECT 'ERROR: @BDATE_TO IS A REQUIRED PARAMETER' RETURN -1 END
IF @RETURN_TYPE IS NULL
  BEGIN SELECT 'ERROR: @RETURN_TYPE IS A REQUIRED PARAMETER' RETURN -1 END
IF @RETURN_TYPE NOT IN ('ACCT', 'BMK', 'MODEL')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @RETURN_TYPE PARAMETER' RETURN -1 END
IF @RETURN_TYPE != 'ACCT' AND @WEIGHT IS NULL
  BEGIN SELECT 'ERROR: @WEIGHT IS A REQUIRED PARAMETER' RETURN -1 END
IF @RETURN_TYPE = 'MODEL' AND @MODEL_DEF_CD IS NULL
  BEGIN SELECT 'ERROR: @MODEL_DEF_CD IS A REQUIRED PARAMETER' RETURN -1 END
IF @RETURN_TYPE = 'ACCT' AND @ACCOUNT_CD IS NULL
  BEGIN SELECT 'ERROR: @ACCOUNT_CD IS A REQUIRED PARAMETER' RETURN -1 END
IF @RETURN_TYPE = 'BMK' AND @BENCHMARK_CD IS NULL
  BEGIN SELECT 'ERROR: @BENCHMARK_CD IS A REQUIRED PARAMETER' RETURN -1 END
IF @STRATEGY_ID IS NULL
  BEGIN SELECT 'ERROR: @STRATEGY_ID IS A REQUIRED PARAMETER' RETURN -1 END

EXEC business_date_get @DIFF=0, @REF_DATE=@BDATE_FROM, @RET_DATE=@BDATE_FROM OUTPUT
EXEC business_date_get @DIFF=0, @REF_DATE=@BDATE_TO, @RET_DATE=@BDATE_TO OUTPUT

IF @RETURN_TYPE = 'ACCT'
BEGIN
  IF EXISTS (SELECT r.* FROM return_calc_params p, return_calc_result r
              WHERE p.bdate_from = @BDATE_FROM
                AND p.bdate_to = @BDATE_TO
                AND p.return_type = 'ACCT'
                AND p.strategy_id = @STRATEGY_ID
                AND p.weight = 'MVAL'
                AND p.account_cd = @ACCOUNT_CD
                AND p.return_calc_id = r.return_calc_id)
  BEGIN
    IF @DEBUG=1 BEGIN SELECT 'WARNING: RETURN CALCULATION HAS BEEN RUN PREVIOUSLY' END
    RETURN 0
  END
  ELSE
  BEGIN
    DELETE return_calc_params
     WHERE bdate_from = @BDATE_FROM
       AND bdate_to = @BDATE_TO
       AND return_type = 'ACCT'
       AND strategy_id = @STRATEGY_ID
       AND weight = 'MVAL'
       AND account_cd = @ACCOUNT_CD
    INSERT return_calc_params (bdate_from, bdate_to, return_type, strategy_id, weight, account_cd, run_tm)
    SELECT @BDATE_FROM, @BDATE_TO, @RETURN_TYPE, @STRATEGY_ID, 'MVAL', @ACCOUNT_CD, GETDATE()
  END
END
ELSE IF @RETURN_TYPE = 'BMK'
BEGIN
  IF EXISTS (SELECT r.* FROM return_calc_params p, return_calc_result r
              WHERE p.bdate_from = @BDATE_FROM
                AND p.bdate_to = @BDATE_TO
                AND p.return_type = 'BMK'
                AND p.strategy_id = @STRATEGY_ID
                AND p.weight = @WEIGHT
                AND p.benchmark_cd = @BENCHMARK_CD
                AND p.return_calc_id = r.return_calc_id)
  BEGIN
    IF @DEBUG=1 BEGIN SELECT 'WARNING: RETURN CALCULATION HAS BEEN RUN PREVIOUSLY' END
    RETURN 0
  END
  ELSE
  BEGIN
    DELETE return_calc_params
     WHERE bdate_from = @BDATE_FROM
       AND bdate_to = @BDATE_TO
       AND return_type = 'BMK'
       AND strategy_id = @STRATEGY_ID
       AND weight = @WEIGHT
       AND benchmark_cd = @BENCHMARK_CD
    INSERT return_calc_params (bdate_from, bdate_to, return_type, strategy_id, weight, benchmark_cd, run_tm)
    SELECT @BDATE_FROM, @BDATE_TO, @RETURN_TYPE, @STRATEGY_ID, @WEIGHT, @BENCHMARK_CD, GETDATE()
  END
END
ELSE IF @RETURN_TYPE = 'MODEL'
BEGIN
  IF EXISTS (SELECT r.* FROM return_calc_params p, return_calc_result r
              WHERE p.bdate_from = @BDATE_FROM
                AND p.bdate_to = @BDATE_TO
                AND p.return_type = 'MODEL'
                AND p.strategy_id = @STRATEGY_ID
                AND p.weight = @WEIGHT
                AND p.model_def_cd = @MODEL_DEF_CD
                AND p.return_calc_id = r.return_calc_id)
  BEGIN
    IF @DEBUG=1 BEGIN SELECT 'WARNING: RETURN CALCULATION HAS BEEN RUN PREVIOUSLY' END
    RETURN 0
  END
  ELSE
  BEGIN
    DELETE return_calc_params
     WHERE bdate_from = @BDATE_FROM
       AND bdate_to = @BDATE_TO
       AND return_type = 'MODEL'
       AND strategy_id = @STRATEGY_ID
       AND weight = @WEIGHT
       AND model_def_cd = @MODEL_DEF_CD
    INSERT return_calc_params (bdate_from, bdate_to, return_type, strategy_id, weight, model_def_cd, run_tm)
    SELECT @BDATE_FROM, @BDATE_TO, @RETURN_TYPE, @STRATEGY_ID, @WEIGHT, @MODEL_DEF_CD, GETDATE()
  END
END

DECLARE @ADATE datetime,
        @CDATE datetime,
        @RETURN_CALC_ID int,
        @SECTOR_MODEL_ID int,
        @SECTOR_NUM int

SELECT @RETURN_CALC_ID = MAX(return_calc_id) FROM return_calc_params

SELECT @SECTOR_MODEL_ID = f.sector_model_id
  FROM strategy g, factor_model f
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = f.factor_model_id

CREATE TABLE #POSITION (
  prev_bdate	datetime	NULL,
  bdate			datetime	NULL,
  univ_type		varchar(16)	NULL,
  sector_id		int			NULL,
  security_id	int			NULL,
  units			float		NULL,
  price			float		NULL,
  mval			float		NULL,
  weight		float		NULL,
  rtn			float		NULL
)

IF @RETURN_TYPE = 'MODEL'
BEGIN
  DECLARE @PERIOD_LEN int, @REBAL_BDATE datetime
  SELECT @PERIOD_LEN = DATEDIFF(DD, @BDATE_FROM, @BDATE_TO)
END
ELSE
  BEGIN EXEC business_date_get @DIFF=-1, @REF_DATE=@BDATE_TO, @RET_DATE=@BDATE_TO OUTPUT END

EXEC business_date_get @DIFF=-1, @REF_DATE=@BDATE_FROM, @RET_DATE=@BDATE_FROM OUTPUT

IF @RETURN_TYPE = 'ACCT'
BEGIN
  INSERT #POSITION (prev_bdate, univ_type, security_id, units)
  SELECT reference_date, 'TOTAL', security_id, SUM(ISNULL(quantity,0.0))
    FROM equity_common..position
   WHERE reference_date >= @BDATE_FROM
     AND reference_date <= @BDATE_TO
     AND reference_date = effective_date
     AND acct_cd IN (SELECT DISTINCT acct_cd FROM equity_common..account WHERE parent = @ACCOUNT_CD OR acct_cd = @ACCOUNT_CD)
     AND security_id IS NOT NULL
   GROUP BY reference_date, security_id

  DELETE #POSITION WHERE units = 0.0
END
ELSE IF @RETURN_TYPE = 'BMK'
BEGIN
  IF @WEIGHT = 'CAP'
  BEGIN
    IF EXISTS (SELECT 1 FROM benchmark WHERE benchmark_cd = @BENCHMARK_CD)
    BEGIN
      INSERT #POSITION (prev_bdate, univ_type, security_id, weight)
      SELECT reference_date, 'TOTAL', security_id, weight
        FROM equity_common..benchmark_weight
       WHERE acct_cd = @BENCHMARK_CD
         AND reference_date >= @BDATE_FROM
         AND reference_date <= @BDATE_TO
         AND reference_date = effective_date
         AND security_id IS NOT NULL
    END
    ELSE
    BEGIN
      INSERT #POSITION (prev_bdate, univ_type, security_id, weight)
      SELECT p.universe_dt, 'TOTAL', p.security_id, p.weight / 100.0
        FROM universe_def d, universe_makeup p
       WHERE d.universe_cd = @BENCHMARK_CD
         AND d.universe_id = p.universe_id
         AND p.universe_dt >= @BDATE_FROM
         AND p.universe_dt <= @BDATE_TO
         AND p.security_id IS NOT NULL
    END
  END
  ELSE IF @WEIGHT = 'EQUAL'
  BEGIN
    IF EXISTS (SELECT 1 FROM benchmark WHERE benchmark_cd = @BENCHMARK_CD)
    BEGIN
      INSERT #POSITION (prev_bdate, univ_type, security_id)
      SELECT reference_date, 'TOTAL', security_id
        FROM equity_common..benchmark_weight
       WHERE acct_cd = @BENCHMARK_CD
         AND reference_date >= @BDATE_FROM
         AND reference_date <= @BDATE_TO
         AND reference_date = effective_date
         AND security_id IS NOT NULL
    END
    ELSE
    BEGIN
      INSERT #POSITION (prev_bdate, univ_type, security_id)
      SELECT p.universe_dt, 'TOTAL', p.security_id
        FROM universe_def d, universe_makeup p
       WHERE d.universe_cd = @BENCHMARK_CD
         AND d.universe_id = p.universe_id
         AND p.universe_dt >= @BDATE_FROM
         AND p.universe_dt <= @BDATE_TO
         AND p.security_id IS NOT NULL
    END
  END
END
ELSE IF @RETURN_TYPE = 'MODEL'
BEGIN
  DECLARE @FRACTILE_NUM int,
          @FRACTILE_SIZE int,
          @DIVISOR int,
          @UPPER_BOUND int,
          @LOWER_BOUND int

  IF @MODEL_DEF_CD LIKE 'Q%'
    BEGIN SELECT @DIVISOR = 5 END
  ELSE IF @MODEL_DEF_CD LIKE 'D%'
    BEGIN SELECT @DIVISOR = 10 END

  SELECT @FRACTILE_SIZE = fractile / @DIVISOR FROM strategy WHERE strategy_id = @STRATEGY_ID
  IF EXISTS (SELECT * FROM strategy WHERE strategy_id = @STRATEGY_ID AND rank_order = 1)
    BEGIN SELECT @FRACTILE_NUM = CONVERT(int, SUBSTRING(@MODEL_DEF_CD, 2, LEN(@MODEL_DEF_CD))) END
  ELSE
    BEGIN SELECT @FRACTILE_NUM = @DIVISOR - CONVERT(int, SUBSTRING(@MODEL_DEF_CD, 2, LEN(@MODEL_DEF_CD))) + 1 END

  SELECT @LOWER_BOUND = @FRACTILE_SIZE * (@DIVISOR - @FRACTILE_NUM) + 1
  SELECT @UPPER_BOUND = @LOWER_BOUND + @FRACTILE_SIZE - 1

  IF @DEBUG = 1
  BEGIN
    SELECT '@DIVISOR', @DIVISOR
    SELECT '@FRACTILE_SIZE', @FRACTILE_SIZE
    SELECT '@FRACTILE_NUM', @FRACTILE_NUM
    SELECT '@UPPER_BOUND', @UPPER_BOUND
    SELECT '@LOWER_BOUND', @LOWER_BOUND
  END

  CREATE TABLE #SS_SECURITY (
    sector_id	int		NULL,
    segment_id	int		NULL,
    security_id	int		NULL
  )

  CREATE TABLE #SCORES (
    security_id		int		NULL,
    sector_score	float	NULL,
    segment_score	float	NULL,
    ss_score		float	NULL,
    universe_score	float	NULL,
    total_score		float	NULL
  )

  SELECT @REBAL_BDATE = @BDATE_FROM
  EXEC business_date_get @DIFF=1, @REF_DATE=@REBAL_BDATE, @RET_DATE=@ADATE OUTPUT

  IF @DEBUG = 1
  BEGIN
    SELECT '@REBAL_BDATE', @REBAL_BDATE
    SELECT '@BDATE_FROM', @BDATE_FROM
    SELECT '@BDATE_TO', @BDATE_TO
  END

  WHILE @ADATE <= @BDATE_TO
  BEGIN
    IF DATEPART(DW, @ADATE) NOT IN (1, 7) AND NOT EXISTS (SELECT * FROM holiday WHERE schedule = 'NYSE' AND [date] = @ADATE)
    BEGIN
      TRUNCATE TABLE #SS_SECURITY
      TRUNCATE TABLE #SCORES

      INSERT #SCORES
            (security_id, sector_score, segment_score, ss_score, universe_score, total_score)
      SELECT security_id, sector_score, segment_score, ss_score, universe_score, total_score
        FROM scores
       WHERE strategy_id = @STRATEGY_ID
         AND bdate = @REBAL_BDATE
         AND security_id IS NOT NULL

      INSERT #SS_SECURITY
      SELECT ss.sector_id, ss.segment_id, ss.security_id
        FROM #SCORES s, sector_model_security ss
       WHERE ss.bdate = @REBAL_BDATE
         AND ss.sector_model_id = @SECTOR_MODEL_ID
         AND ss.security_id = s.security_id
         AND ss.security_id IS NOT NULL
         AND s.security_id IS NOT NULL

      IF NOT EXISTS (SELECT * FROM #SS_SECURITY)
      BEGIN
        INSERT #SS_SECURITY
        SELECT DISTINCT NULL, NULL, security_id
          FROM #SCORES
      END

      IF @DEBUG = 1
      BEGIN
        SELECT '#SCORES: INITIAL STATE'
        SELECT * FROM #SCORES ORDER BY security_id
        SELECT '#SS_SECURITY'
        SELECT * FROM #SS_SECURITY ORDER BY security_id
      END

      IF EXISTS (SELECT * FROM decode WHERE item='NO REFRACTILE' AND code='TOTAL_SCORE' AND decode=@STRATEGY_ID) AND EXISTS (SELECT * FROM #SCORES)
      BEGIN
        EXEC scores_temp_rank_update @BDATE=@REBAL_BDATE, @SCORE_TYPE='TOTAL_SCORE', @STRATEGY_ID=@STRATEGY_ID, @DEBUG=@DEBUG
        IF @DEBUG = 1
        BEGIN
          SELECT '#SCORES: AFTER REFRACTILE TOTAL SCORE'
          SELECT * FROM #SCORES ORDER BY security_id
        END
      END

      INSERT #POSITION (bdate, univ_type, security_id)
      SELECT @ADATE, 'TOTAL', security_id
        FROM #SCORES
       WHERE total_score <= @UPPER_BOUND
         AND total_score >= @LOWER_BOUND

      IF EXISTS (SELECT * FROM decode WHERE item='NO REFRACTILE' AND code='UNIVERSE_SCORE' AND decode=@STRATEGY_ID) AND EXISTS (SELECT * FROM #SCORES)
      BEGIN
        EXEC scores_temp_rank_update @BDATE=@REBAL_BDATE, @SCORE_TYPE='UNIVERSE_SCORE', @STRATEGY_ID=@STRATEGY_ID, @DEBUG=@DEBUG
        IF @DEBUG = 1
        BEGIN
          SELECT '#SCORES: AFTER REFRACTILE UNIVERSE SCORE'
          SELECT * FROM #SCORES ORDER BY security_id
        END
      END

      INSERT #POSITION (bdate, univ_type, security_id)
      SELECT @ADATE, 'UNIVERSE', security_id
        FROM #SCORES
       WHERE universe_score <= @UPPER_BOUND
         AND universe_score >= @LOWER_BOUND

      IF EXISTS (SELECT * FROM decode WHERE item='NO REFRACTILE' AND code='SECTOR_SCORE' AND decode=@STRATEGY_ID) AND EXISTS (SELECT * FROM #SCORES)
      BEGIN
        EXEC scores_temp_rank_update @BDATE=@REBAL_BDATE, @SCORE_TYPE='SECTOR_SCORE', @STRATEGY_ID=@STRATEGY_ID, @DEBUG=@DEBUG
        IF @DEBUG = 1
        BEGIN
          SELECT '#SCORES: AFTER REFRACTILE SECTOR SCORE'
          SELECT * FROM #SCORES ORDER BY security_id
        END
      END

      INSERT #POSITION (bdate, univ_type, security_id)
      SELECT @ADATE, 'SECTOR', security_id
        FROM #SCORES
       WHERE sector_score <= @UPPER_BOUND
         AND sector_score >= @LOWER_BOUND

      SELECT @SECTOR_NUM = 0
      WHILE EXISTS (SELECT * FROM sector_def WHERE sector_model_id = @SECTOR_MODEL_ID AND sector_num > @SECTOR_NUM)
      BEGIN
        SELECT @SECTOR_NUM = MIN(sector_num)
          FROM sector_def
         WHERE sector_model_id = @SECTOR_MODEL_ID
           AND sector_num > @SECTOR_NUM

        INSERT #POSITION (bdate, univ_type, sector_id, security_id)
        SELECT @ADATE, 'SECTOR'+CONVERT(varchar,@SECTOR_NUM), d.sector_id, s.security_id
          FROM #SCORES s, sector_model_security ss, sector_def d
         WHERE s.sector_score <= @UPPER_BOUND
           AND s.sector_score >= @LOWER_BOUND
           AND ss.bdate = @REBAL_BDATE
           AND s.security_id = ss.security_id
           AND ss.sector_model_id = @SECTOR_MODEL_ID
           AND ss.sector_id = d.sector_id
           AND d.sector_num = @SECTOR_NUM
      END

      SELECT @SECTOR_NUM = @SECTOR_NUM + 1

      INSERT #POSITION (bdate, univ_type, sector_id, security_id)
      SELECT @ADATE, 'SECTOR'+CONVERT(varchar,@SECTOR_NUM), -1, s.security_id
        FROM #SCORES s, sector_model_security ss
       WHERE s.sector_score <= @UPPER_BOUND
         AND s.sector_score >= @LOWER_BOUND
         AND ss.bdate = @REBAL_BDATE
         AND s.security_id = ss.security_id
         AND ss.sector_model_id = @SECTOR_MODEL_ID
         AND ss.sector_id IS NULL
    END

    IF @PERIOD_LEN > 4
    BEGIN
      IF DATEPART(DD, DATEADD(DD, 1, @ADATE)) = 1
      BEGIN
        SELECT @REBAL_BDATE = @ADATE
        IF DATEPART(DW, @REBAL_BDATE) IN (1, 7) OR EXISTS (SELECT * FROM holiday WHERE schedule = 'NYSE' AND [date] = @REBAL_BDATE)
          BEGIN EXEC business_date_get @DIFF=-1, @REF_DATE=@REBAL_BDATE, @RET_DATE=@REBAL_BDATE OUTPUT END
      END
    END

    SELECT @ADATE = DATEADD(DD, 1, @ADATE)
  END

  DROP TABLE #SS_SECURITY
  DROP TABLE #SCORES
END

IF @DEBUG = 1
BEGIN
  SELECT '#POSITION (1)'
  SELECT * FROM #POSITION ORDER BY prev_bdate, bdate, univ_type, security_id
END

/**** CHECK AND LOAD ANY SECURITIES THAT ARE NOT YET CLASSIFIED: BEGIN ****/
IF @RETURN_TYPE != 'MODEL'
BEGIN
  DECLARE @DUMMY_UNIVERSE_ID int
  SELECT @DUMMY_UNIVERSE_ID = universe_id FROM universe_def WHERE universe_cd = 'DUMMY'

  INSERT universe_makeup (universe_dt, universe_id, security_id)
  SELECT DISTINCT p.prev_bdate, @DUMMY_UNIVERSE_ID, p.security_id
    FROM #POSITION p
   WHERE NOT EXISTS (SELECT 1 FROM sector_model_security ss
                      WHERE ss.bdate = p.prev_bdate
                        AND ss.sector_model_id = @SECTOR_MODEL_ID
                        AND ss.security_id = p.security_Id)

  IF @DEBUG = 1
  BEGIN
    SELECT 'DUMMY UNIVERSE TO BE CLASSIFIED'
    SELECT * FROM universe_makeup
     WHERE universe_id = @DUMMY_UNIVERSE_ID
     ORDER BY universe_dt, security_id
  END

  SELECT @ADATE = '1/1/1990'
  WHILE EXISTS (SELECT * FROM universe_makeup WHERE universe_id = @DUMMY_UNIVERSE_ID AND universe_dt > @ADATE)
  BEGIN
    SELECT @ADATE = MIN(universe_dt)
      FROM universe_makeup
     WHERE universe_id = @DUMMY_UNIVERSE_ID
       AND universe_dt > @ADATE

    EXEC sector_model_security_populate @BDATE=@ADATE, @SECTOR_MODEL_ID=@SECTOR_MODEL_ID, @UNIVERSE_DT=@ADATE, @UNIVERSE_ID=@DUMMY_UNIVERSE_ID
  END

  DELETE universe_makeup
   WHERE universe_id = @DUMMY_UNIVERSE_ID
END
/**** CHECK AND LOAD ANY SECURITIES THAT ARE NOT YET CLASSIFIED: END ****/

/**** LOAD SECTORS FOR ACCT AND BMK: BEGIN ****/
IF @RETURN_TYPE = 'ACCT'
BEGIN
  SELECT @SECTOR_NUM = 0
  WHILE EXISTS (SELECT * FROM sector_def WHERE sector_model_id = @SECTOR_MODEL_ID AND sector_num > @SECTOR_NUM)
  BEGIN
    SELECT @SECTOR_NUM = MIN(sector_num)
      FROM sector_def
     WHERE sector_model_id = @SECTOR_MODEL_ID
       AND sector_num > @SECTOR_NUM

    INSERT #POSITION (prev_bdate, univ_type, sector_id, security_id, units)
    SELECT p.prev_bdate, 'SECTOR'+CONVERT(varchar,@SECTOR_NUM), d.sector_id, p.security_id, p.units
      FROM #POSITION p, sector_model_security ss, sector_def d
     WHERE p.univ_type = 'TOTAL'
       AND p.prev_bdate = ss.bdate
       AND p.security_id = ss.security_id
       AND ss.sector_model_id = @SECTOR_MODEL_ID
       AND ss.sector_id = d.sector_id
       AND d.sector_num = @SECTOR_NUM
  END

  SELECT @SECTOR_NUM = @SECTOR_NUM + 1

  INSERT #POSITION (prev_bdate, univ_type, sector_id, security_id, units)
  SELECT p.prev_bdate, 'SECTOR'+CONVERT(varchar,@SECTOR_NUM), -1, p.security_id, p.units
    FROM #POSITION p, sector_model_security ss
   WHERE p.univ_type = 'TOTAL'
     AND p.prev_bdate = ss.bdate
     AND p.security_id = ss.security_id
     AND ss.sector_model_id = @SECTOR_MODEL_ID
     AND ss.sector_id IS NULL
END
IF @RETURN_TYPE = 'BMK'
BEGIN
  SELECT @SECTOR_NUM = 0
  WHILE EXISTS (SELECT * FROM sector_def WHERE sector_model_id = @SECTOR_MODEL_ID AND sector_num > @SECTOR_NUM)
  BEGIN
    SELECT @SECTOR_NUM = MIN(sector_num)
      FROM sector_def
     WHERE sector_model_id = @SECTOR_MODEL_ID
       AND sector_num > @SECTOR_NUM

    INSERT #POSITION (prev_bdate, univ_type, sector_id, security_id, weight)
    SELECT p.prev_bdate, 'SECTOR'+CONVERT(varchar,@SECTOR_NUM), d.sector_id, p.security_id, p.weight
      FROM #POSITION p, sector_model_security ss, sector_def d
     WHERE p.univ_type = 'TOTAL'
       AND p.prev_bdate = ss.bdate
       AND p.security_id = ss.security_id
       AND ss.sector_model_id = @SECTOR_MODEL_ID
       AND ss.sector_id = d.sector_id
       AND d.sector_num = @SECTOR_NUM
  END

  SELECT @SECTOR_NUM = @SECTOR_NUM + 1

  INSERT #POSITION (prev_bdate, univ_type, sector_id, security_id, weight)
  SELECT p.prev_bdate, 'SECTOR'+CONVERT(varchar,@SECTOR_NUM), -1, p.security_id, p.weight
    FROM #POSITION p, sector_model_security ss
   WHERE p.univ_type = 'TOTAL'
     AND p.prev_bdate = ss.bdate
     AND p.security_id = ss.security_id
     AND ss.sector_model_id = @SECTOR_MODEL_ID
     AND ss.sector_id IS NULL
END
/**** LOAD SECTORS FOR ACCT AND BMK: END ****/

IF @DEBUG = 1
BEGIN
  SELECT '#POSITION (2)'
  SELECT * FROM #POSITION ORDER BY prev_bdate, bdate, univ_type, security_id
END

/**** UPDATE DATES: BEGIN ****/
WHILE EXISTS (SELECT * FROM #POSITION WHERE bdate IS NULL)
BEGIN
  SELECT @ADATE = MIN(prev_bdate) FROM #POSITION WHERE bdate IS NULL
  EXEC business_date_get @DIFF=1, @REF_DATE=@ADATE, @RET_DATE=@CDATE OUTPUT
  UPDATE #POSITION SET bdate = @CDATE WHERE prev_bdate = @ADATE
END

UPDATE #POSITION
   SET prev_bdate = x.prev_bdate
  FROM (SELECT bdate, prev_bdate FROM #POSITION WHERE prev_bdate IS NOT NULL) x
 WHERE #POSITION.prev_bdate IS NULL
   AND #POSITION.bdate = x.bdate

WHILE EXISTS (SELECT * FROM #POSITION WHERE prev_bdate IS NULL)
BEGIN
  SELECT @ADATE = MIN(bdate) FROM #POSITION WHERE prev_bdate IS NULL
  EXEC business_date_get @DIFF=-1, @REF_DATE=@ADATE, @RET_DATE=@CDATE OUTPUT
  UPDATE #POSITION SET prev_bdate = @CDATE WHERE bdate = @ADATE
END
/**** UPDATE DATES: END****/

IF @DEBUG = 1
BEGIN
  SELECT '#POSITION (3)'
  SELECT * FROM #POSITION ORDER BY prev_bdate, bdate, univ_type, security_id
END

/**** UPDATE WEIGHTS: BEGIN ****/
IF @RETURN_TYPE = 'ACCT'
BEGIN
  UPDATE #POSITION
     SET price = p.price_close_usd
    FROM equity_common..market_price p
   WHERE #POSITION.prev_bdate = p.reference_date
     AND #POSITION.security_id = p.security_id

  UPDATE #POSITION SET price = 0.0 WHERE price IS NULL
  UPDATE #POSITION SET mval = units * price

  UPDATE #POSITION --ACCOUNTS ARE ALWAYS MARKET-VALUE WEIGHTED
     SET weight = mval / x.tot_mval
    FROM (SELECT prev_bdate, univ_type, SUM(mval) AS tot_mval
            FROM #POSITION GROUP BY prev_bdate, univ_type) x
   WHERE #POSITION.prev_bdate = x.prev_bdate
     AND #POSITION.univ_type = x.univ_type
END

IF @WEIGHT = 'EQUAL'
BEGIN
  UPDATE #POSITION
     SET weight = 1.0 / x.cnt
    FROM (SELECT prev_bdate, univ_type, COUNT(*) AS cnt
            FROM #POSITION GROUP BY prev_bdate, univ_type) x
   WHERE #POSITION.prev_bdate = x.prev_bdate
     AND #POSITION.univ_type = x.univ_type
END
ELSE IF @WEIGHT = 'CAP'
BEGIN
  UPDATE #POSITION
     SET mval = p.market_cap_usd
    FROM equity_common..market_price p
   WHERE #POSITION.prev_bdate = p.reference_date
     AND #POSITION.security_id = p.security_id

  IF @RETURN_TYPE = 'MODEL'
  BEGIN
    UPDATE #POSITION
       SET weight = mval / x.tot_mval
      FROM (SELECT prev_bdate, univ_type, SUM(mval) AS tot_mval
              FROM #POSITION GROUP BY prev_bdate, univ_type) x
     WHERE #POSITION.prev_bdate = x.prev_bdate
       AND #POSITION.univ_type = x.univ_type
  END
  ELSE IF @RETURN_TYPE = 'BMK'
  BEGIN
    UPDATE #POSITION
       SET weight = weight / x.tot_weight
      FROM (SELECT prev_bdate, univ_type, SUM(weight) AS tot_weight
              FROM #POSITION
             WHERE univ_type != 'TOTAL' --TOTAL_BMK CAP WEIGHTS ARE FROM UNIVERSE_MAKEUP TABLE AND ALREADY SET PREVIOUSLY
             GROUP BY prev_bdate, univ_type) x
     WHERE #POSITION.prev_bdate = x.prev_bdate
       AND #POSITION.univ_type = x.univ_type
  END
END
/**** UPDATE WEIGHTS: END ****/

IF @DEBUG = 1
BEGIN
  SELECT '#POSITION (4)'
  SELECT * FROM #POSITION ORDER BY prev_bdate, bdate, univ_type, security_id
END

CREATE TABLE #SECURITY (
  bdate			datetime	NULL,
  security_id	int			NULL,
  rtn			float		NULL
)

INSERT #SECURITY
SELECT i.bdate, i.security_id, i.factor_value
  FROM factor f, instrument_factor i,
      (SELECT DISTINCT bdate, security_id FROM #POSITION) x
 WHERE f.factor_cd = 'RETURN_1D'
   AND f.factor_id = i.factor_id
   AND x.bdate = i.bdate
   AND x.security_id = i.security_id

IF @DEBUG = 1
BEGIN
  SELECT '#SECURITY'
  SELECT * FROM #SECURITY ORDER BY bdate, security_id
END

UPDATE #POSITION
   SET rtn = #POSITION.weight * s.rtn
  FROM #SECURITY s
 WHERE #POSITION.bdate = s.bdate
   AND #POSITION.security_id = s.security_id

DROP TABLE #SECURITY

UPDATE #POSITION
   SET rtn = 0.0
 WHERE rtn IS NULL

IF @DEBUG = 1
BEGIN
  SELECT '#POSITION (5)'
  SELECT * FROM #POSITION ORDER BY prev_bdate, bdate, univ_type, security_id
END

CREATE TABLE #DLY_RTN (
  bdate		datetime	NULL,
  univ_type	varchar(16)	NULL,
  rtn_p1	float		NULL
)

IF @RETURN_TYPE != 'BMK'
  BEGIN CREATE CLUSTERED INDEX IX_temp_dly_rtn ON #DLY_RTN (bdate, univ_type) END

INSERT #DLY_RTN
SELECT bdate, univ_type, SUM(rtn) + 1.0
  FROM #POSITION
 GROUP BY bdate, univ_type

DROP TABLE #POSITION

IF @RETURN_TYPE = 'BMK'
  BEGIN CREATE CLUSTERED INDEX IX_temp_dly_rtn ON #DLY_RTN (bdate, univ_type) END

IF @DEBUG = 1
BEGIN
  SELECT '#DLY_RTN'
  SELECT * FROM #DLY_RTN ORDER BY bdate, univ_type
END

CREATE TABLE #RETURN_CALC_RESULT (
  ordinal		int identity(1,1)	NOT NULL,
  univ_type		varchar(32)			NOT NULL,
  sector_model_id	int				NULL,
  sector_id		int					NULL,
  segment_id	int					NULL,
  rtn			float				NOT NULL
)

DECLARE @TOTAL_RTN float,
        @UNIVERSE_RTN float,
        @SECTOR_RTN float

/**** CALCULATE TOTAL, UNIVERSE, AND SECTOR RETURNS: BEGIN ****/
SELECT @TOTAL_RTN=1.0, @UNIVERSE_RTN=1.0, @SECTOR_RTN=1.0
SELECT @ADATE = '1/1/1990'
WHILE EXISTS (SELECT * FROM #DLY_RTN WHERE bdate > @ADATE)
BEGIN
  SELECT @ADATE = MIN(bdate) FROM #DLY_RTN WHERE bdate > @ADATE
  SELECT @TOTAL_RTN = @TOTAL_RTN * rtn_p1 FROM #DLY_RTN WHERE univ_type = 'TOTAL' AND bdate = @ADATE
  IF @RETURN_TYPE = 'MODEL'
  BEGIN
    SELECT @UNIVERSE_RTN = @UNIVERSE_RTN * rtn_p1 FROM #DLY_RTN WHERE univ_type = 'UNIVERSE' AND bdate = @ADATE
    SELECT @SECTOR_RTN = @SECTOR_RTN * rtn_p1 FROM #DLY_RTN WHERE univ_type = 'SECTOR' AND bdate = @ADATE
  END
END

SELECT @TOTAL_RTN = @TOTAL_RTN - 1.0,
       @UNIVERSE_RTN = @UNIVERSE_RTN - 1.0,
       @SECTOR_RTN = @SECTOR_RTN - 1.0

INSERT #RETURN_CALC_RESULT (univ_type, rtn)
SELECT 'TOTAL', @TOTAL_RTN

IF @RETURN_TYPE = 'MODEL'
BEGIN
  INSERT #RETURN_CALC_RESULT (univ_type, rtn)
  SELECT 'UNIVERSE', @UNIVERSE_RTN

  INSERT #RETURN_CALC_RESULT (univ_type, rtn)
  SELECT 'SECTOR', @SECTOR_RTN
END
/**** CALCULATE TOTAL, UNIVERSE, AND SECTOR RETURNS: END ****/

IF @DEBUG = 1
BEGIN
  SELECT '#RETURN_CALC_RESULT (1)'
  SELECT * FROM #RETURN_CALC_RESULT ORDER BY ordinal
END

/**** CALCULATE INDIVIDUAL SECTOR RETURNS: BEGIN ****/
SELECT @SECTOR_NUM = 0
WHILE EXISTS (SELECT * FROM sector_def WHERE sector_model_id = @SECTOR_MODEL_ID AND sector_num > @SECTOR_NUM)
BEGIN
  SELECT @SECTOR_NUM = MIN(sector_num)
    FROM sector_def
   WHERE sector_model_id = @SECTOR_MODEL_ID
     AND sector_num > @SECTOR_NUM

  SELECT @SECTOR_RTN=1.0
  SELECT @ADATE = '1/1/1990'
  WHILE EXISTS (SELECT * FROM #DLY_RTN WHERE bdate > @ADATE)
  BEGIN
    SELECT @ADATE = MIN(bdate) FROM #DLY_RTN WHERE bdate > @ADATE
    SELECT @SECTOR_RTN = @SECTOR_RTN * rtn_p1 FROM #DLY_RTN WHERE univ_type = 'SECTOR'+CONVERT(varchar,@SECTOR_NUM) AND bdate = @ADATE
  END

  SELECT @SECTOR_RTN = @SECTOR_RTN - 1.0

  INSERT #RETURN_CALC_RESULT (univ_type, sector_model_id, sector_id, rtn)
  SELECT 'SECTOR', @SECTOR_MODEL_ID, sector_id, @SECTOR_RTN
    FROM sector_def
   WHERE sector_model_id = @SECTOR_MODEL_ID
     AND sector_num = @SECTOR_NUM
END

SELECT @SECTOR_NUM = @SECTOR_NUM + 1
IF EXISTS (SELECT * FROM #DLY_RTN WHERE univ_type LIKE 'SECTOR'+CONVERT(varchar,@SECTOR_NUM)+'%')
BEGIN
  SELECT @SECTOR_RTN=1.0
  SELECT @ADATE = '1/1/1990'
  WHILE EXISTS (SELECT * FROM #DLY_RTN WHERE bdate > @ADATE)
  BEGIN
    SELECT @ADATE = MIN(bdate) FROM #DLY_RTN WHERE bdate > @ADATE
    SELECT @SECTOR_RTN = @SECTOR_RTN * rtn_p1 FROM #DLY_RTN WHERE univ_type = 'SECTOR'+CONVERT(varchar,@SECTOR_NUM) AND bdate = @ADATE
  END

  SELECT @SECTOR_RTN = @SECTOR_RTN - 1.0

  INSERT #RETURN_CALC_RESULT (univ_type, sector_model_id, rtn)
  SELECT 'SECTOR', @SECTOR_MODEL_ID, @SECTOR_RTN
END
/**** CALCULATE INDIVIDUAL SECTOR RETURNS: END ****/

DROP TABLE #DLY_RTN

IF @DEBUG = 1
BEGIN
  SELECT '#RETURN_CALC_RESULT (2)'
  SELECT * FROM #RETURN_CALC_RESULT ORDER BY ordinal
END

INSERT return_calc_result
SELECT @RETURN_CALC_ID, univ_type, sector_model_id, sector_id, segment_id, rtn
  FROM #RETURN_CALC_RESULT
 ORDER BY ordinal

DROP TABLE #RETURN_CALC_RESULT

RETURN 0
go
IF OBJECT_ID('dbo.return_calculate') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.return_calculate >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.return_calculate >>>'
go
