use QER
go
IF OBJECT_ID('dbo.rpt_prm_get_return_params') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rpt_prm_get_return_params
    IF OBJECT_ID('dbo.rpt_prm_get_return_params') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rpt_prm_get_return_params >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rpt_prm_get_return_params >>>'
END
go
CREATE PROCEDURE dbo.rpt_prm_get_return_params @RETURN_CALC_DAILY_ID int
AS

SELECT r.return_calc_daily_id, r.return_type, r.strategy_id, r.weight, r.account_cd, r.benchmark_cd, m.model_portfolio_def_cd, r.period_type, r.periods
  FROM return_calc_params_daily r, model_portfolio_def m
 WHERE r.return_calc_daily_id = @RETURN_CALC_DAILY_ID
   AND r.model_portfolio_def_id = m.model_portfolio_def_id

RETURN 0
go
IF OBJECT_ID('dbo.rpt_prm_get_return_params') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_prm_get_return_params >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_prm_get_return_params >>>'
go
