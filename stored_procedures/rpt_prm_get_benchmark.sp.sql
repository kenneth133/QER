use QER
go
IF OBJECT_ID('dbo.rpt_prm_get_benchmark') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rpt_prm_get_benchmark
    IF OBJECT_ID('dbo.rpt_prm_get_benchmark') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rpt_prm_get_benchmark >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rpt_prm_get_benchmark >>>'
END
go
CREATE PROCEDURE dbo.rpt_prm_get_benchmark @STRATEGY_ID int = NULL,
                                           @ACCOUNT_CD varchar(32) = NULL
AS

IF @STRATEGY_ID IS NOT NULL AND @ACCOUNT_CD IS NOT NULL
BEGIN
  SELECT DISTINCT benchmark_cd
    FROM account
   WHERE account_cd = @ACCOUNT_CD
     AND strategy_id = @STRATEGY_ID
   ORDER BY benchmark_cd
END
ELSE IF @STRATEGY_ID IS NOT NULL
BEGIN
  SELECT benchmark_cd
    FROM account
   WHERE strategy_id = @STRATEGY_ID
   UNION
  SELECT d.universe_cd AS [benchmark_cd]
    FROM strategy g, universe_def d
   WHERE g.strategy_id = @STRATEGY_ID
     AND g.universe_id = d.universe_id
   ORDER BY benchmark_cd
END
ELSE
BEGIN
  SELECT DISTINCT benchmark_cd
    FROM benchmark
   ORDER BY benchmark_cd
END

RETURN 0
go
IF OBJECT_ID('dbo.rpt_prm_get_benchmark') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_prm_get_benchmark >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_prm_get_benchmark >>>'
go
