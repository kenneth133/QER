use QER
go
IF OBJECT_ID('dbo.return_calc_daily') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.return_calc_daily
    IF OBJECT_ID('dbo.return_calc_daily') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.return_calc_daily >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.return_calc_daily >>>'
END
go
CREATE PROCEDURE dbo.return_calc_daily @BDATE datetime,
                                       @RETURN_CALC_DAILY_ID int = NULL,
                                       @DEBUG bit = NULL
AS

DECLARE @RETURN_TYPE varchar(32),
        @STRATEGY_ID int,
        @WEIGHT varchar(16),
        @ACCOUNT_CD varchar(32),
        @BENCHMARK_CD varchar(50),
        @MODEL_PORTFOLIO_DEF_CD varchar(32),
        @PERIOD_TYPE varchar(2),
        @PERIODS int

IF @RETURN_CALC_DAILY_ID IS NULL
BEGIN
  SELECT @RETURN_CALC_DAILY_ID = 0
  WHILE EXISTS (SELECT * FROM return_calc_params_daily WHERE return_calc_daily_id > @RETURN_CALC_DAILY_ID)
  BEGIN
    SELECT @RETURN_CALC_DAILY_ID = MIN(return_calc_daily_id)
      FROM return_calc_params_daily
     WHERE return_calc_daily_id > @RETURN_CALC_DAILY_ID

    SELECT @RETURN_TYPE = r.return_type,
           @STRATEGY_ID = r.strategy_id,
           @WEIGHT = r.weight,
           @ACCOUNT_CD = r.account_cd,
           @BENCHMARK_CD = r.benchmark_cd,
           @MODEL_PORTFOLIO_DEF_CD = m.model_portfolio_def_cd,
           @PERIOD_TYPE = r.period_type,
           @PERIODS = r.periods
      FROM return_calc_params_daily r, model_portfolio_def m
     WHERE r.return_calc_daily_id = @RETURN_CALC_DAILY_ID
       AND r.model_portfolio_def_id = m.model_portfolio_def_id

    IF @DEBUG = 1
    BEGIN
      SELECT '@RETURN_CALC_DAILY_ID', @RETURN_CALC_DAILY_ID
      SELECT '@RETURN_TYPE', @RETURN_TYPE
      SELECT '@STRATEGY_ID', @STRATEGY_ID
      SELECT '@WEIGHT', @WEIGHT
      SELECT '@ACCOUNT_CD', @ACCOUNT_CD
      SELECT '@BENCHMARK_CD', @BENCHMARK_CD
      SELECT '@MODEL_PORTFOLIO_DEF_CD', @MODEL_PORTFOLIO_DEF_CD
      SELECT '@PERIOD_TYPE', @PERIOD_TYPE
      SELECT '@PERIODS', @PERIODS
    END

    EXEC rpt_model_performance @BDATE = @BDATE,
                               @RETURN_TYPE = @RETURN_TYPE,
                               @STRATEGY_ID = @STRATEGY_ID,
                               @WEIGHT = @WEIGHT,
                               @ACCOUNT_CD = @ACCOUNT_CD,
                               @BENCHMARK_CD = @BENCHMARK_CD,
                               @MODEL_PORTFOLIO_DEF_CD = @MODEL_PORTFOLIO_DEF_CD,
                               @PERIOD_TYPE = @PERIOD_TYPE,
                               @PERIODS = @PERIODS,
                               @DEBUG = @DEBUG
  END
END
ELSE
BEGIN
  SELECT @RETURN_TYPE = r.return_type,
         @STRATEGY_ID = r.strategy_id,
         @WEIGHT = r.weight,
         @ACCOUNT_CD = r.account_cd,
         @BENCHMARK_CD = r.benchmark_cd,
         @MODEL_PORTFOLIO_DEF_CD = m.model_portfolio_def_cd,
         @PERIOD_TYPE = r.period_type,
         @PERIODS = r.periods
    FROM return_calc_params_daily r, model_portfolio_def m
   WHERE r.return_calc_daily_id = @RETURN_CALC_DAILY_ID
     AND r.model_portfolio_def_id = m.model_portfolio_def_id

  IF @DEBUG = 1
  BEGIN
    SELECT '@RETURN_CALC_DAILY_ID', @RETURN_CALC_DAILY_ID
    SELECT '@RETURN_TYPE', @RETURN_TYPE
    SELECT '@STRATEGY_ID', @STRATEGY_ID
    SELECT '@WEIGHT', @WEIGHT
    SELECT '@ACCOUNT_CD', @ACCOUNT_CD
    SELECT '@BENCHMARK_CD', @BENCHMARK_CD
    SELECT '@MODEL_PORTFOLIO_DEF_CD', @MODEL_PORTFOLIO_DEF_CD
    SELECT '@PERIOD_TYPE', @PERIOD_TYPE
    SELECT '@PERIODS', @PERIODS
  END

  EXEC rpt_model_performance @BDATE = @BDATE,
                             @RETURN_TYPE = @RETURN_TYPE,
                             @STRATEGY_ID = @STRATEGY_ID,
                             @WEIGHT = @WEIGHT,
                             @ACCOUNT_CD = @ACCOUNT_CD,
                             @BENCHMARK_CD = @BENCHMARK_CD,
                             @MODEL_PORTFOLIO_DEF_CD = @MODEL_PORTFOLIO_DEF_CD,
                             @PERIOD_TYPE = @PERIOD_TYPE,
                             @PERIODS = @PERIODS,
                             @DEBUG = @DEBUG
END

RETURN 0
go
IF OBJECT_ID('dbo.return_calc_daily') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.return_calc_daily >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.return_calc_daily >>>'
go
