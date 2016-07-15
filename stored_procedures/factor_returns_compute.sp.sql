use QER
go
IF OBJECT_ID('dbo.factor_returns_compute') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.factor_returns_compute
    IF OBJECT_ID('dbo.factor_returns_compute') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.factor_returns_compute >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.factor_returns_compute >>>'
END
go
CREATE PROCEDURE dbo.factor_returns_compute @BDATE datetime,				--required
                                            @AS_OF_DATE datetime = NULL,		--optional, defaults to getdate()
                                            @UNIVERSE_CD varchar(16),			--required
                                            @FACTOR_CD varchar(64),			--required
                                            @FACTOR_SOURCE_CD varchar(8) = NULL,	--optional, defaults to latest update_tm
                                            @RETURN_FACTOR_CD varchar(64),		--required
                                            @RETURN_FACTOR_SOURCE_CD varchar(8) = NULL,	--optional, defaults to latest update_tm
                                            @GROUPS int = NULL,				--optional, defaults to 10
                                            @AGAINST varchar(1) = NULL,			--optional, defaults to U, values: U,C,G
                                            @SECTOR_MODEL_ID int = NULL,		--optional if @AGAINST = U, required otherwise
                                            @AGAINST_NUM int = NULL,			--optional if @AGAINST = U, required otherwise, such as sector_num or segment_num
                                            @RANK_WGT_ID int = NULL,			--optional, for smooth ranking
                                            @PERIOD_TYPE varchar(2) = NULL,		--optional if @RANK_WGT_ID = NULL, required otherwise, values: YY,QQ,Q,MM,M,WK,WW,DD,D
                                            @METHOD varchar(4) = NULL,			--optional, defaults to MEAN
                                            @MISSING_METHOD varchar(8) = NULL,		--optional, defaults to MEDIAN
                                            @MISSING_VALUE float = NULL,		--optional, defaults to NULL; if not NULL, overrides @MISSING_METHOD
                                            @DEBUG bit = NULL				--optional, for debugging
AS

DECLARE @FACTOR_RETURN_EVENT_ID	int,
	@FACTOR_ID		int,
        @RETURN_FACTOR_ID	int,
        @UNIVERSE_ID		int,
        @UNIVERSE_DT		datetime,
        @RUN_TM			datetime

SELECT @RUN_TM = getdate()
SELECT @UNIVERSE_CD = upper(@UNIVERSE_CD)
SELECT @FACTOR_CD = upper(@FACTOR_CD)
SELECT @FACTOR_SOURCE_CD = upper(@FACTOR_SOURCE_CD)
SELECT @AGAINST = upper(@AGAINST)
SELECT @PERIOD_TYPE = upper(@PERIOD_TYPE)
SELECT @METHOD = upper(@METHOD)
SELECT @MISSING_METHOD = upper(@MISSING_METHOD)
SELECT @GROUPS = abs(@GROUPS)

IF @BDATE IS NULL BEGIN SELECT 'ERROR: @BDATE IS A REQUIRED PARAMETER' RETURN -1 END
IF @AS_OF_DATE IS NULL BEGIN SELECT @AS_OF_DATE = @RUN_TM END
IF @UNIVERSE_CD IS NULL BEGIN SELECT 'ERROR: @UNIVERSE_CD IS A REQUIRED PARAMETER' RETURN -1 END
IF @FACTOR_CD IS NULL BEGIN SELECT 'ERROR: @FACTOR_CD IS A REQUIRED PARAMETER' RETURN -1 END
IF @RETURN_FACTOR_CD IS NULL BEGIN SELECT 'ERROR: @RETURN_FACTOR_CD IS A REQUIRED PARAMETER' RETURN -1 END

IF @FACTOR_SOURCE_CD IS NOT NULL AND NOT EXISTS (SELECT * FROM QER..instrument_factor WHERE source_cd = @FACTOR_SOURCE_CD)
BEGIN SELECT 'ERROR: @FACTOR_SOURCE_CD = ' + @FACTOR_SOURCE_CD + ' NOT FOUND IN QER..instrument_factor TABLE' RETURN -1 END
IF @RETURN_FACTOR_SOURCE_CD IS NOT NULL AND NOT EXISTS (SELECT * FROM QER..instrument_factor WHERE source_cd = @RETURN_FACTOR_SOURCE_CD)
BEGIN SELECT 'ERROR: @RETURN_FACTOR_SOURCE_CD = ' + @RETURN_FACTOR_SOURCE_CD + ' NOT FOUND IN QER..instrument_factor TABLE' RETURN -1 END

IF @GROUPS IS NULL BEGIN SELECT @GROUPS = 10 END
IF @AGAINST IS NULL BEGIN SELECT @AGAINST = 'U' END

IF @AGAINST IS NOT NULL AND @AGAINST NOT IN ('U','C','G')
BEGIN SELECT 'ERROR: @AGAINST MUST BE ONE OF THE FOLLOWING: U,C,G' RETURN -1 END
IF @AGAINST != 'U' AND @SECTOR_MODEL_ID IS NULL
BEGIN SELECT 'ERROR: @SECTOR_MODEL_ID IS A REQUIRED PARAMETER WHEN RANKING AGAINST SECTOR OR SEGMENT' RETURN -1 END
IF @AGAINST != 'U' AND @AGAINST_NUM IS NULL
BEGIN SELECT 'ERROR: @AGAINST_NUM IS A REQUIRED PARAMETER WHEN RANKING AGAINST SECTOR OR SEGMENT' RETURN -1 END

IF @RANK_WGT_ID IS NOT NULL AND NOT EXISTS (SELECT * FROM QER..rank_weight WHERE rank_wgt_id = @RANK_WGT_ID)
BEGIN SELECT 'ERROR: @RANK_WGT_ID = ' + @RANK_WGT_ID + ' NOT FOUND IN QER..rank_weight TABLE' RETURN -1 END
IF @RANK_WGT_ID IS NOT NULL AND @PERIOD_TYPE IS NULL
BEGIN SELECT 'ERROR: @PERIOD_TYPE IS A REQUIRED PARAMETER WHEN SMOOTH RANKING' RETURN -1 END
IF @RANK_WGT_ID IS NOT NULL AND @PERIOD_TYPE IS NOT NULL
BEGIN
  IF @PERIOD_TYPE NOT IN ('YY','YYYY','QQ','Q','MM','M','WK','WW','DD','D')
  SELECT 'ERROR: @PERIOD_TYPE MUST BE ONE OF THE FOLLOWING: YY,YYYY,QQ,Q,MM,M,WK,WW,DD,D'
  RETURN -1
END

IF @METHOD IS NOT NULL
BEGIN
  IF @METHOD != 'MEAN' AND @METHOD NOT LIKE 'HI%' AND @METHOD NOT LIKE 'LO%'
  BEGIN SELECT 'ERROR: "' + @METHOD + '" IS NOT A VALID VALUE FOR @METHOD PARAMETER' RETURN -1 END
END
ELSE BEGIN SELECT @METHOD = 'MEAN' END
IF @MISSING_METHOD IS NOT NULL AND @MISSING_VALUE IS NULL
BEGIN
  IF @MISSING_METHOD NOT IN ('MODE','MEDIAN','MIN','MAX')
  BEGIN SELECT 'ERROR: "' + @MISSING_METHOD + '" IS NOT A VALID VALUE FOR @MISSING_METHOD PARAMETER' RETURN -1 END
END
IF @MISSING_METHOD IS NULL BEGIN SELECT @MISSING_METHOD = 'MEDIAN' END

SELECT @FACTOR_ID = factor_id
  FROM QER..factor
 WHERE factor_cd = @FACTOR_CD

SELECT @RETURN_FACTOR_ID = factor_id
  FROM QER..factor
 WHERE factor_cd = @RETURN_FACTOR_CD

SELECT @UNIVERSE_ID = universe_id
  FROM QER..universe_def
 WHERE universe_cd = @UNIVERSE_CD

IF @FACTOR_ID IS NULL BEGIN SELECT 'ERROR: @FACTOR_CD = ' + @FACTOR_CD + ' NOT FOUND IN QER..factor TABLE' RETURN -1 END
IF @RETURN_FACTOR_ID IS NULL BEGIN SELECT 'ERROR: @RETURN_FACTOR_CD = ' + @RETURN_FACTOR_CD + ' NOT FOUND IN QER..factor TABLE' RETURN -1 END
IF @UNIVERSE_ID IS NULL BEGIN SELECT 'ERROR: @UNIVERSE_CD = ' + @UNIVERSE_CD + ' NOT FOUND IN QER..universe_def TABLE' RETURN -1 END

--SELECT @UNIVERSE_DT = @BDATE
--EXEC QER..instrument_factor_bdate_get @FACTOR_ID, @FACTOR_SOURCE_CD, @UNIVERSE_ID, @UNIVERSE_DT, @BDATE OUTPUT

SELECT @UNIVERSE_DT = @BDATE
EXEC QER..universe_date_get @UNIVERSE_ID, @UNIVERSE_DT OUTPUT

BEGIN TRAN
  INSERT factor_return_inputs (run_tm, as_of_date, bdate, universe_dt, universe_id, factor_id, factor_source_cd,
                               return_factor_id, return_factor_source_cd, groups, 
                               sector_model_id, against, against_num, rank_wgt_id, period_type,
                               method, missing_method, missing_value)
  SELECT @RUN_TM, @AS_OF_DATE, @BDATE, @UNIVERSE_DT, @UNIVERSE_ID, @FACTOR_ID, @FACTOR_SOURCE_CD, 
         @RETURN_FACTOR_ID, @RETURN_FACTOR_SOURCE_CD, @GROUPS,
         @SECTOR_MODEL_ID, @AGAINST, @AGAINST_NUM, @RANK_WGT_ID, @PERIOD_TYPE,
         @METHOD, @MISSING_METHOD, @MISSING_VALUE

  SELECT @FACTOR_RETURN_EVENT_ID = max(factor_return_event_id)
    FROM QER..factor_return_inputs
COMMIT TRAN

IF @DEBUG = 1
BEGIN
  SELECT 'QER..factor_return_inputs: factor_returns_compute'
  SELECT * FROM QER..factor_return_inputs
   WHERE factor_return_event_id = @FACTOR_RETURN_EVENT_ID
END

CREATE TABLE #ENTIRE_SET (
  mqa_id	varchar(32)		NULL,
  cusip8	varchar(32)		NULL,
  ticker	varchar(16)		NULL,
  gv_key	int			NULL,
  mktcap	float			NULL,
  factor_value	float			NULL,
  ordinal	int identity(1,1)	NOT NULL,
  rank		int			NULL,
  eq_return	float			NULL,
  cap_return	float			NULL
)

EXEC rank_against_populate @UNIVERSE_DT, @UNIVERSE_ID, @SECTOR_MODEL_ID, @AGAINST, @AGAINST_NUM, @DEBUG
EXEC rank_factor_populate @BDATE, @AS_OF_DATE, @FACTOR_ID, @FACTOR_SOURCE_CD, @RANK_WGT_ID, @PERIOD_TYPE, @MISSING_METHOD, @MISSING_VALUE, @DEBUG
EXEC rank_factor_compute @GROUPS, @METHOD, @DEBUG
EXEC factor_returns_populate @BDATE, @AS_OF_DATE, @RETURN_FACTOR_ID, @RETURN_FACTOR_SOURCE_CD, @DEBUG

CREATE TABLE #RANK_XILE_MAP (
  rank		int	NOT NULL,
  xile		int	NOT NULL
)

CREATE TABLE #XILE_RETURNS (
  xile		int	NOT NULL,
  eq_return	float	NOT NULL,
  cap_return	float	NOT NULL
)

DECLARE @COUNTER	int,
        @TOTAL_MKTCAP	float

SELECT @COUNTER = 1
SELECT @GROUPS = @GROUPS - 1

WHILE @GROUPS >= 0
BEGIN
  INSERT #RANK_XILE_MAP
  SELECT @GROUPS, @COUNTER

  SELECT @COUNTER = @COUNTER + 1
  SELECT @GROUPS = @GROUPS - 1
END

IF @DEBUG = 1
BEGIN
  SELECT '#RANK_XILE_MAP: factor_returns_compute'
  SELECT * FROM #RANK_XILE_MAP
END

INSERT #XILE_RETURNS
SELECT r.xile, avg(e.eq_return), sum(e.cap_return)
  FROM #RANK_XILE_MAP r, #ENTIRE_SET e
 WHERE r.rank = e.rank
 GROUP BY r.xile

SELECT @TOTAL_MKTCAP = sum(mktcap)
  FROM #ENTIRE_SET

INSERT #XILE_RETURNS
SELECT 0, avg(eq_return), sum(eq_return * (mktcap/@TOTAL_MKTCAP))
  FROM #ENTIRE_SET

INSERT QER..factor_return_output
SELECT @FACTOR_RETURN_EVENT_ID, xile, eq_return, cap_return
  FROM #XILE_RETURNS
 ORDER BY xile

DROP TABLE #ENTIRE_SET
DROP TABLE #RANK_XILE_MAP
DROP TABLE #XILE_RETURNS

RETURN 0
go
IF OBJECT_ID('dbo.factor_returns_compute') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.factor_returns_compute >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.factor_returns_compute >>>'
go