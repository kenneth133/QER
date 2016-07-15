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
CREATE PROCEDURE dbo.strategy_ranks_run @BDATE datetime = NULL, --optional, defaults to previous business day
                                        @STRATEGY_ID int = NULL  --required
AS

IF @STRATEGY_ID IS NULL
BEGIN
  SELECT 'ERROR: @STRATEGY_ID PARAMETER MUST BE PASSED'
  RETURN -1
END

IF @BDATE IS NULL
BEGIN
  EXEC business_date_get @DIFF=-1, @RET_DATE=@BDATE OUTPUT
END

DECLARE @FACTOR_MODEL_ID	int,
        @FACTOR_ID		int,
        @UNIVERSE_ID		int,
        @GROUPS			int,
        @AGAINST		varchar(1),
        @AGAINST_ID		int

SELECT @UNIVERSE_ID = universe_id,
       @FACTOR_MODEL_ID = factor_model_id,
       @GROUPS = fractile
  FROM QER..strategy
 WHERE strategy_id = @STRATEGY_ID

CREATE TABLE #RANK_PARAMETERS (
  i		int identity(1,1)	NOT NULL,
  factor_id	int			NOT NULL,
  against	varchar(1)		NOT NULL,
  against_id	int			NULL
)

INSERT #RANK_PARAMETERS (factor_id, against, against_id)
SELECT factor_id, against, against_id
  FROM QER..factor_against_weight
 WHERE factor_model_id = @FACTOR_MODEL_ID

WHILE EXISTS (SELECT * FROM #RANK_PARAMETERS)
BEGIN
  SELECT @FACTOR_ID = factor_id,
         @AGAINST = against,
         @AGAINST_ID = against_id
    FROM #RANK_PARAMETERS
   WHERE i = (SELECT min(i) FROM #RANK_PARAMETERS)

  IF @AGAINST IN ('C','G')
  BEGIN
    EXEC rank_factor_universe @BDATE = @BDATE, @UNIVERSE_ID = @UNIVERSE_ID, @FACTOR_ID = @FACTOR_ID,
                              @GROUPS = @GROUPS, @AGAINST = @AGAINST, @AGAINST_ID = @AGAINST_ID
  END
  ELSE IF @AGAINST IN ('U')
  BEGIN
    EXEC rank_factor_universe @BDATE = @BDATE, @UNIVERSE_ID = @UNIVERSE_ID, @FACTOR_ID = @FACTOR_ID,
                              @GROUPS = @GROUPS, @AGAINST = @AGAINST
  END

  DELETE #RANK_PARAMETERS
   WHERE i = (SELECT min(i) FROM #RANK_PARAMETERS)
END

RETURN 0
go
IF OBJECT_ID('dbo.strategy_ranks_run') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.strategy_ranks_run >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.strategy_ranks_run >>>'
go