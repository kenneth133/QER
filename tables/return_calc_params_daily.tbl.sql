use QER
go

create table dbo.return_calc_params_daily (
  return_calc_daily_id	int identity(1,1) not null primary key,
  return_type		varchar(32)	not null,
  strategy_id		int		not null,
  weight		varchar(16)	not null,
  account_cd		varchar(32)	null,
  bm_universe_id	int		null,
  model_portfolio_def_id int		null,
  period_type		varchar(2)	not null,
  periods		int		not null
)
go
