use QER
go
IF OBJECT_ID('dbo.strategy_security_classify') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.strategy_security_classify
    IF OBJECT_ID('dbo.strategy_security_classify') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.strategy_security_classify >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.strategy_security_classify >>>'
END
go
CREATE PROCEDURE dbo.strategy_security_classify
@BDATE datetime,
@STRATEGY_ID int,
@DEBUG bit = NULL
AS

DECLARE @SECTOR_MODEL_ID int,
        @DUMMY_UNIVERSE_ID int

SELECT @SECTOR_MODEL_ID = m.sector_model_id
  FROM strategy g, factor_model m
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.factor_model_id = m.factor_model_id

SELECT @DUMMY_UNIVERSE_ID = universe_id FROM universe_def WHERE universe_cd = 'DUMMY'

INSERT universe_makeup (universe_dt, universe_id, security_id)
SELECT @BDATE, @DUMMY_UNIVERSE_ID, security_id FROM equity_common..position
 WHERE reference_date = @BDATE
   AND reference_date = effective_date
   AND acct_cd IN (SELECT acct_cd AS [account_cd] FROM equity_common..account
                    WHERE parent IN (SELECT account_cd FROM account WHERE strategy_id=@STRATEGY_ID)
                   UNION
                   SELECT acct_cd AS [account_cd] FROM equity_common..account
                    WHERE acct_cd IN (SELECT account_cd FROM account WHERE strategy_id=@STRATEGY_ID)
                   UNION
                   SELECT benchmark_cd AS [account_cd] FROM account WHERE strategy_id = @STRATEGY_ID)
UNION
SELECT @BDATE, @DUMMY_UNIVERSE_ID, security_id FROM universe_makeup
 WHERE universe_dt = @BDATE
   AND universe_id IN (SELECT universe_id FROM strategy WHERE strategy_id = @STRATEGY_ID
                       UNION
                       SELECT universe_id FROM universe_def
                        WHERE universe_cd IN (SELECT benchmark_cd FROM account WHERE strategy_id = @STRATEGY_ID))

IF @DEBUG = 1
BEGIN
  SELECT 'DUMMY UNIVERSE TO BE CLASSIFIED'
  SELECT * FROM universe_makeup
   WHERE universe_id = @DUMMY_UNIVERSE_ID
   ORDER BY universe_dt, security_id
END

EXEC sector_model_security_populate @BDATE=@BDATE, @SECTOR_MODEL_ID=@SECTOR_MODEL_ID, @UNIVERSE_DT=@BDATE, @UNIVERSE_ID=@DUMMY_UNIVERSE_ID, @DEBUG=@DEBUG

DELETE universe_makeup
 WHERE universe_dt = universe_dt
   AND universe_id = @DUMMY_UNIVERSE_ID

RETURN 0
go
IF OBJECT_ID('dbo.strategy_security_classify') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.strategy_security_classify >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.strategy_security_classify >>>'
go
