use QER
go
IF OBJECT_ID('dbo.rank_factor_universe') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rank_factor_universe
    IF OBJECT_ID('dbo.rank_factor_universe') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rank_factor_universe >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rank_factor_universe >>>'
END
go
CREATE PROCEDURE dbo.rank_factor_universe
@BDATE datetime,			--required
@AS_OF_DATE datetime = NULL,--optional, defaults to getdate()
@UNIVERSE_ID int,			--required
@FACTOR_ID int,				--required
@FACTOR_SOURCE_CD varchar(8) = NULL,--optional, defaults to latest update_tm regardless of source
@GROUPS int = NULL,			--optional, defaults to 100
@AGAINST varchar(1) = NULL,	--optional, defaults to U; values: U,C,G
@AGAINST_CD varchar(8) = NULL,--optional if @AGAINST in 'U,C,G'; required if @AGAINST = 'Y'
@AGAINST_ID int = NULL,		--optional if @AGAINST = U, required otherwise; values: sector_id or segment_id
@RANK_WGT_ID int = NULL,	--optional, for smooth ranking
@PERIOD_TYPE varchar(2) = NULL,--optional if @RANK_WGT_ID = NULL, required otherwise, values: YY,QQ,Q,MM,M,WK,WW,DD,D
@METHOD varchar(4) = NULL,	--optional, defaults to MEAN; values: MEAN,HI%,LO%
@MISSING_METHOD varchar(8) = NULL,--optional, defaults to MEDIAN; values: MODE,MEDIAN,MIN,MAX
@MISSING_VALUE float = NULL,--optional, defaults to NULL; if not NULL, overrides @MISSING_METHOD
@DEBUG bit = NULL			--optional, for debugging
AS

DECLARE @RANK_EVENT_ID	int,
        @RUN_TM		datetime

SELECT @RUN_TM = GETDATE()
SELECT @FACTOR_SOURCE_CD = UPPER(@FACTOR_SOURCE_CD)
SELECT @AGAINST = UPPER(@AGAINST)
SELECT @PERIOD_TYPE = UPPER(@PERIOD_TYPE)
SELECT @METHOD = UPPER(@METHOD)
SELECT @MISSING_METHOD = UPPER(@MISSING_METHOD)
SELECT @GROUPS = ABS(@GROUPS)

IF @BDATE IS NULL BEGIN SELECT 'ERROR: @BDATE IS A REQUIRED PARAMETER' RETURN -1 END
IF @AS_OF_DATE IS NULL BEGIN SELECT @AS_OF_DATE = @RUN_TM END

IF @UNIVERSE_ID IS NULL BEGIN SELECT 'ERROR: @UNIVERSE_ID IS A REQUIRED PARAMETER' RETURN -1 END
IF NOT EXISTS (SELECT * FROM universe_def WHERE universe_id = @UNIVERSE_ID)
  BEGIN SELECT 'ERROR: @UNIVERSE_ID = ' + @UNIVERSE_ID + ' NOT FOUND IN universe_def TABLE' RETURN -1 END

IF @FACTOR_ID IS NULL BEGIN SELECT 'ERROR: @FACTOR_ID IS A REQUIRED PARAMETER' RETURN -1 END
IF NOT EXISTS (SELECT * FROM factor WHERE factor_id = @FACTOR_ID)
  BEGIN SELECT 'ERROR: @FACTOR_ID = ' + @FACTOR_ID + ' NOT FOUND IN factor TABLE' RETURN -1 END
IF @FACTOR_SOURCE_CD IS NOT NULL AND NOT EXISTS (SELECT * FROM instrument_factor WHERE source_cd = @FACTOR_SOURCE_CD)
  BEGIN SELECT 'ERROR: @FACTOR_SOURCE_CD = ' + @FACTOR_SOURCE_CD + ' NOT FOUND IN instrument_factor TABLE' RETURN -1 END

IF @GROUPS IS NULL BEGIN SELECT @GROUPS = 100 END
IF @AGAINST IS NULL BEGIN SELECT @AGAINST = 'U' END

IF @AGAINST IS NOT NULL AND @AGAINST NOT IN ('U','Y','C','G')
  BEGIN SELECT 'ERROR: @AGAINST MUST BE ONE OF THE FOLLOWING: U, C, G' RETURN -1 END
IF @AGAINST IN ('C','G') AND @AGAINST_ID IS NULL
  BEGIN SELECT 'ERROR: @AGAINST_ID IS A REQUIRED PARAMETER WHEN RANKING AGAINST SECTOR OR SEGMENT' RETURN -1 END
IF @AGAINST = 'Y' AND @AGAINST_CD IS NULL
  BEGIN SELECT 'ERROR: @AGAINST_CD IS A REQUIRED PARAMETER WHEN RANKING AGAINST COUNTRY' RETURN -1 END

IF @RANK_WGT_ID IS NOT NULL AND NOT EXISTS (SELECT * FROM rank_weight WHERE rank_wgt_id = @RANK_WGT_ID)
  BEGIN SELECT 'ERROR: @RANK_WGT_ID = ' + @RANK_WGT_ID + ' NOT FOUND IN rank_weight TABLE' RETURN -1 END
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

INSERT rank_inputs (run_tm, as_of_date, bdate, universe_id,
                    factor_id, factor_source_cd, groups,
                    against, against_cd, against_id, rank_wgt_id, period_type,
                    method, missing_method, missing_value)
SELECT @RUN_TM, @AS_OF_DATE, @BDATE, @UNIVERSE_ID,
       @FACTOR_ID, @FACTOR_SOURCE_CD, @GROUPS,
       @AGAINST, @AGAINST_CD, @AGAINST_ID, @RANK_WGT_ID, @PERIOD_TYPE,
       @METHOD, @MISSING_METHOD, @MISSING_VALUE

SELECT @RANK_EVENT_ID = MAX(rank_event_id)
  FROM rank_inputs

IF @DEBUG = 1
BEGIN
  SELECT 'rank_inputs: rank_factor_universe'
  SELECT * FROM rank_inputs
   WHERE rank_event_id = @RANK_EVENT_ID
END

CREATE TABLE #DATA_SET (
  security_id int		NULL,
  mkt_cap	float		NULL,
  factor_value float	NULL,
  ordinal	int identity(1,1) NOT NULL,
  rank		int			NULL,
  eq_return	float		NULL,
  cap_return float		NULL
)

EXEC rank_against_populate @RANK_EVENT_ID, @DEBUG
EXEC rank_factor_populate @RANK_EVENT_ID, @DEBUG
EXEC rank_factor_compute @RANK_EVENT_ID, @DEBUG

INSERT rank_output
SELECT @RANK_EVENT_ID, security_id, factor_value, rank
  FROM #DATA_SET
 ORDER BY rank, factor_value, security_id

DROP TABLE #DATA_SET

RETURN 0
go
IF OBJECT_ID('dbo.rank_factor_universe') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rank_factor_universe >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rank_factor_universe >>>'
go
