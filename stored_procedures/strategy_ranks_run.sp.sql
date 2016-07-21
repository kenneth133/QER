use QER
go
IF OBJECT_ID('dbo.strategy_ranks_run') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.strategy_ranks_run
    IF OBJECT_ID('dbo.strategy_ranks_run') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.strategy_ranks_run >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.strategy_ranks_run >>>'
END
go
CREATE PROCEDURE dbo.strategy_ranks_run
@BDATE datetime = NULL, --optional, defaults to previous business day
@STRATEGY_ID int, --required
@DEBUG bit = NULL
AS

IF @STRATEGY_ID IS NULL
  BEGIN SELECT 'ERROR: @STRATEGY_ID PARAMETER MUST BE PASSED' RETURN -1 END

IF @BDATE IS NULL
  BEGIN EXEC business_date_get @DIFF=-1, @RET_DATE=@BDATE OUTPUT END

DECLARE
@FACTOR_MODEL_ID	int,
@FACTOR_ID			int,
@UNIVERSE_ID		int,
@GROUPS				int,
@AGAINST			varchar(1),
@AGAINST_CD			varchar(8),
@AGAINST_ID			int,
@ORDINAL			int

SELECT @UNIVERSE_ID = universe_id,
       @FACTOR_MODEL_ID = factor_model_id,
       @GROUPS = fractile
  FROM strategy
 WHERE strategy_id = @STRATEGY_ID

IF @DEBUG = 1
BEGIN
  SELECT '@BDATE', @BDATE
  SELECT '@UNIVERSE_ID', @UNIVERSE_ID
  SELECT '@FACTOR_MODEL_ID', @FACTOR_MODEL_ID
  SELECT '@GROUPS', @GROUPS
END

CREATE TABLE #RANK_PARAMETERS (
  ordinal		int identity(1,1)	NOT NULL,
  factor_id		int					NOT NULL,
  against		varchar(1)			NOT NULL,
  against_cd	varchar(8)			NULL,
  against_id	int					NULL
)

INSERT #RANK_PARAMETERS (factor_id, against, against_id)
SELECT factor_id, against, against_id
  FROM factor_against_weight
 WHERE factor_model_id = @FACTOR_MODEL_ID
   AND against != 'Y'

INSERT #RANK_PARAMETERS (factor_id, against, against_cd)
SELECT DISTINCT w.factor_id, w.against, ISNULL(y.domicile_iso_cd, y.issue_country_cd)
  FROM factor_against_weight w, universe_makeup p, equity_common..security y
 WHERE w.factor_model_id = @FACTOR_MODEL_ID
   AND w.against = 'Y'
   AND p.universe_dt = @BDATE
   AND p.universe_id = @UNIVERSE_ID
   AND p.security_id = y.security_id

IF @DEBUG = 1
BEGIN
  SELECT '#RANK_PARAMETERS'
  SELECT * FROM #RANK_PARAMETERS ORDER BY ordinal
END

SELECT @ORDINAL = 0
WHILE EXISTS (SELECT 1 FROM #RANK_PARAMETERS WHERE ordinal > @ORDINAL)
BEGIN
  SELECT @ORDINAL = MIN(ordinal) FROM #RANK_PARAMETERS WHERE ordinal > @ORDINAL

  SELECT @FACTOR_ID = factor_id,
         @AGAINST = against,
         @AGAINST_CD = against_cd,
         @AGAINST_ID = against_id
    FROM #RANK_PARAMETERS
   WHERE ordinal = @ORDINAL

  EXEC rank_factor_universe @BDATE=@BDATE, @UNIVERSE_ID=@UNIVERSE_ID, @FACTOR_ID=@FACTOR_ID,
  @GROUPS=@GROUPS, @AGAINST=@AGAINST, @AGAINST_CD=@AGAINST_CD, @AGAINST_ID=@AGAINST_ID, @DEBUG=@DEBUG
END

DROP TABLE #RANK_PARAMETERS

RETURN 0
go
IF OBJECT_ID('dbo.strategy_ranks_run') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.strategy_ranks_run >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.strategy_ranks_run >>>'
go
