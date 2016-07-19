use QER
go

create table dbo.return_calc_params (
  return_calc_id	int identity(1,1) not null	primary key,
  bdate_from		datetime	not null,
  bdate_to		datetime	not null,
  return_type		varchar(32)	not null,
  strategy_id		int		not null,
  weight		varchar(16)	not null,

  account_cd		varchar(32)	null,
  benchmark_cd		varchar(50)	null,
  model_def_cd		varchar(32)	null,
  run_tm		datetime	not null
)
go

CREATE NONCLUSTERED INDEX IX_return_calc_params ON return_calc_params (bdate_from, bdate_to, return_type)
go
