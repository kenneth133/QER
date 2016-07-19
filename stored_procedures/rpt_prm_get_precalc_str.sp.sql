use QER
go
IF OBJECT_ID('dbo.rpt_prm_get_precalc_str') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rpt_prm_get_precalc_str
    IF OBJECT_ID('dbo.rpt_prm_get_precalc_str') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rpt_prm_get_precalc_str >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rpt_prm_get_precalc_str >>>'
END
go
CREATE PROCEDURE dbo.rpt_prm_get_precalc_str @STRATEGY_ID int
AS

CREATE TABLE #RESULT (
  return_calc_daily_id int		not null,
  return_type	varchar(32)		not null,
  strategy_id	int				not null,
  weight		varchar(16)		not null,
  account_cd	varchar(32)		null,
  benchmark_cd	varchar(50)		null,
  model_portfolio_def_cd varchar(32) null,
  period_type	varchar(2)		not null,
  periods		int				not null,
  rtn_str		varchar(255)	null
)

INSERT #RESULT (return_calc_daily_id, return_type, strategy_id, weight, account_cd, benchmark_cd, model_portfolio_def_cd, period_type, periods)
SELECT r.return_calc_daily_id, r.return_type, r.strategy_id, r.weight, r.account_cd, r.benchmark_cd, m.model_portfolio_def_cd, r.period_type, r.periods
  FROM return_calc_params_daily r, model_portfolio_def m
 WHERE r.strategy_id = @STRATEGY_ID
   AND r.model_portfolio_def_id = m.model_portfolio_def_id

UPDATE #RESULT
   SET rtn_str = account_cd + ' (Mval-Weighted, '
 WHERE return_type = 'ACCT'

UPDATE #RESULT
   SET rtn_str = benchmark_cd + ' ('
 WHERE #RESULT.return_type = 'BMK'

UPDATE #RESULT
   SET rtn_str = model_portfolio_def_cd + ' ('
 WHERE return_type = 'MODEL'

UPDATE #RESULT
   SET rtn_str = account_cd + '-' + benchmark_cd + ' ('
 WHERE #RESULT.return_type = 'ACCT-BMK'

UPDATE #RESULT
   SET rtn_str = model_portfolio_def_cd + '-' + benchmark_cd + ' ('
 WHERE #RESULT.return_type = 'MODEL-BMK'

UPDATE #RESULT
   SET rtn_str = account_cd + '-' + model_portfolio_def_cd + ' ('
 WHERE return_type = 'ACCT-MODEL'

UPDATE #RESULT
   SET rtn_str = rtn_str + 'Cap-Weighted, '
 WHERE weight = 'CAP'

UPDATE #RESULT
   SET rtn_str = rtn_str + 'Equal-Weighted, '
 WHERE weight = 'EQUAL'

UPDATE #RESULT
   SET rtn_str = rtn_str + 'Daily, Past ' + CONVERT(varchar,periods) + ' days)'
 WHERE period_type IN ('D','DD')

UPDATE #RESULT
   SET rtn_str = rtn_str + 'Weekly, Past ' + CONVERT(varchar,periods) + ' weeks)'
 WHERE period_type IN ('WK','WW')

UPDATE #RESULT
   SET rtn_str = rtn_str + 'Monthly, Past ' + CONVERT(varchar,periods) + ' months)'
 WHERE period_type IN ('M','MM')

UPDATE #RESULT
   SET rtn_str = rtn_str + 'Quarterly, Past ' + CONVERT(varchar,periods) + ' quarters)'
 WHERE period_type IN ('Q','QQ')

UPDATE #RESULT
   SET rtn_str = rtn_str + 'Yearly, Past ' + CONVERT(varchar,periods) + ' years)'
 WHERE period_type IN ('YY','YYYY')

SELECT return_calc_daily_id, rtn_str
  FROM #RESULT
 ORDER BY return_calc_daily_id

DROP TABLE #RESULT

RETURN 0
go
IF OBJECT_ID('dbo.rpt_prm_get_precalc_str') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_prm_get_precalc_str >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_prm_get_precalc_str >>>'
go
