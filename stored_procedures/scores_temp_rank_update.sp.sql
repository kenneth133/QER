use QER
go

CREATE TABLE #SECURITY_CLASS (
  security_id	int			NULL,
  sector_id		int			NULL,
  segment_id	int			NULL,
  region_id		int			NULL,
  country_cd	varchar(8)	NULL
)

CREATE TABLE #SCORES (
  security_id		int		NULL,
  sector_score		float	NULL,
  segment_score		float	NULL,
  ss_score			float	NULL,
  universe_score	float	NULL,
  country_score		float	NULL,
  region_score		float	NULL,
  total_score		float	NULL
)

IF OBJECT_ID('dbo.scores_temp_rank_update') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.scores_temp_rank_update
    IF OBJECT_ID('dbo.scores_temp_rank_update') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.scores_temp_rank_update >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.scores_temp_rank_update >>>'
END
go
CREATE PROCEDURE dbo.scores_temp_rank_update
@BDATE datetime = NULL, --optional, defaults to previous business day
@SCORE_TYPE varchar(16) = NULL, --required
@STRATEGY_ID int = NULL, --required
@DEBUG bit = NULL
AS

SELECT @SCORE_TYPE = UPPER(@SCORE_TYPE)

IF @SCORE_TYPE IS NULL OR (@SCORE_TYPE NOT IN ('SECTOR_SCORE','SEGMENT_SCORE','SS_SCORE','UNIVERSE_SCORE','COUNTRY_SCORE','REGION_SCORE','TOTAL_SCORE'))
  BEGIN SELECT 'ERROR: @SCORE_TYPE PARAMETER MUST BE PASSED' RETURN -1 END
IF @STRATEGY_ID IS NULL
  BEGIN SELECT 'ERROR: @STRATEGY_ID PARAMETER MUST BE PASSED' RETURN -1 END

DECLARE @AGAINST varchar(1),
        @AGAINST_CD varchar(8),
        @AGAINST_ID int,
        @FACTOR_ID int,
        @GROUPS int,
        @UNIVERSE_ID int,
        @RANK_EVENT_ID_MIN int,
        @RANK_EVENT_ID_MAX int,
        @NOW datetime,
        @ORDINAL int

SELECT @GROUPS = fractile
  FROM strategy
 WHERE strategy_id = @STRATEGY_ID

IF @BDATE IS NULL
  BEGIN EXEC business_date_get @DIFF=-1, @RET_DATE=@BDATE OUTPUT END

SELECT @UNIVERSE_ID = universe_id
  FROM strategy
 WHERE strategy_id = @STRATEGY_ID

SELECT @FACTOR_ID = factor_id
  FROM factor
 WHERE factor_cd = 'DUMMY'

SELECT @NOW = GETDATE()

EXEC dummy_data_delete

INSERT instrument_factor
SELECT @BDATE, security_id, @FACTOR_ID,
       CASE @SCORE_TYPE WHEN 'SECTOR_SCORE' THEN sector_score
                        WHEN 'SEGMENT_SCORE' THEN segment_score
                        WHEN 'SS_SCORE' THEN ss_score
                        WHEN 'UNIVERSE_SCORE' THEN universe_score
                        WHEN 'COUNTRY_SCORE' THEN country_score
                        WHEN 'REGION_SCORE' THEN region_score
                        WHEN 'TOTAL_SCORE' THEN total_score END,
       @NOW, 'FS'
  FROM #SCORES

IF @DEBUG = 1
BEGIN
  SELECT 'instrument_factor'
  SELECT * FROM instrument_factor WHERE bdate=@BDATE AND factor_id=@FACTOR_ID ORDER BY security_id
END

SELECT @RANK_EVENT_ID_MIN = MAX(rank_event_id) + 1
  FROM rank_inputs

IF @SCORE_TYPE IN ('SECTOR_SCORE', 'SEGMENT_SCORE', 'SS_SCORE', 'COUNTRY_SCORE', 'REGION_SCORE')
BEGIN
  CREATE TABLE #AGAINST (
    ordinal		int identity(1,1) NOT NULL,
    against_id	int			NULL,
    against_cd	varchar(8)	NULL
  )

  IF @SCORE_TYPE IN ('SECTOR_SCORE', 'SS_SCORE')
  BEGIN
    SELECT @AGAINST = 'C'

    INSERT #AGAINST (against_id)
    SELECT DISTINCT sector_id FROM #SECURITY_CLASS
     WHERE sector_id IS NOT NULL
  END
  ELSE IF @SCORE_TYPE = 'SEGMENT_SCORE'
  BEGIN
    SELECT @AGAINST = 'G'

    INSERT #AGAINST (against_id)
    SELECT DISTINCT segment_id FROM #SECURITY_CLASS
     WHERE segment_id IS NOT NULL
  END
  ELSE IF @SCORE_TYPE = 'COUNTRY_SCORE'
  BEGIN
    SELECT @AGAINST = 'Y'

    INSERT #AGAINST (against_cd)
    SELECT DISTINCT country_cd FROM #SECURITY_CLASS
     WHERE country_cd IS NOT NULL
  END
  ELSE IF @SCORE_TYPE = 'REGION_SCORE'
  BEGIN
    SELECT @AGAINST = 'R'

    INSERT #AGAINST (against_id)
    SELECT DISTINCT region_id FROM #SECURITY_CLASS
     WHERE region_id IS NOT NULL
  END

  SELECT @ORDINAL = 0
  WHILE EXISTS (SELECT * FROM #AGAINST WHERE ordinal > @ORDINAL)
  BEGIN
    SELECT @ORDINAL = MIN(ordinal)
      FROM #AGAINST
     WHERE ordinal > @ORDINAL

    SELECT @AGAINST_ID = against_id,
           @AGAINST_CD = against_cd
      FROM #AGAINST
     WHERE ordinal = @ORDINAL

    EXEC rank_factor_universe @BDATE=@BDATE, @UNIVERSE_ID=@UNIVERSE_ID, @FACTOR_ID=@FACTOR_ID, @GROUPS=@GROUPS,
                              @AGAINST=@AGAINST, @AGAINST_CD=@AGAINST_CD, @AGAINST_ID=@AGAINST_ID, @DEBUG=@DEBUG
  END

  DROP TABLE #AGAINST
END
ELSE --('UNIVERSE_SCORE', 'TOTAL_SCORE')
BEGIN
  EXEC rank_factor_universe @BDATE=@BDATE, @UNIVERSE_ID=@UNIVERSE_ID, @FACTOR_ID=@FACTOR_ID,
                            @GROUPS=@GROUPS, @AGAINST='U', @DEBUG=@DEBUG
END

SELECT @RANK_EVENT_ID_MAX = MAX(rank_event_id)
  FROM rank_inputs

UPDATE #SCORES
   SET sector_score = CASE @SCORE_TYPE WHEN 'SECTOR_SCORE' THEN o.rank ELSE #SCORES.sector_score END,
       segment_score = CASE @SCORE_TYPE WHEN 'SEGMENT_SCORE' THEN o.rank ELSE #SCORES.segment_score END,
       ss_score = CASE @SCORE_TYPE WHEN 'SS_SCORE' THEN o.rank ELSE #SCORES.ss_score END,
       universe_score = CASE @SCORE_TYPE WHEN 'UNIVERSE_SCORE' THEN o.rank ELSE #SCORES.universe_score END,
       country_score = CASE @SCORE_TYPE WHEN 'COUNTRY_SCORE' THEN o.rank ELSE #SCORES.country_score END,
       region_score = CASE @SCORE_TYPE WHEN 'REGION_SCORE' THEN o.rank ELSE #SCORES.country_score END,
       total_score = CASE @SCORE_TYPE WHEN 'TOTAL_SCORE' THEN o.rank ELSE #SCORES.total_score END
  FROM rank_output o
 WHERE o.rank_event_id >= @RANK_EVENT_ID_MIN
   AND o.rank_event_id <= @RANK_EVENT_ID_MAX
   AND o.security_id = #SCORES.security_id

IF @DEBUG = 1
BEGIN
  SELECT '#SCORES'
  SELECT * FROM #SCORES ORDER BY security_id
END

EXEC dummy_data_delete

RETURN 0
go
IF OBJECT_ID('dbo.scores_temp_rank_update') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.scores_temp_rank_update >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.scores_temp_rank_update >>>'
go

DROP TABLE #SCORES
DROP TABLE #SECURITY_CLASS
go
