use QER
go

create table dbo.strategy (
  strategy_id			int identity(1,1) not null primary key,
  strategy_cd			varchar(16)	null,
  strategy_nm			varchar(64)	null,

  universe_id			int		not null,
  factor_model_id		int		not null,
  fractile			int		not null,
  rank_order			bit		not null,
  model_portfolio_def_id	int		null,
  region_model_id		int		null
)
go
