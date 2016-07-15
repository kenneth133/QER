use QER
go

create table dbo.account (
  strategy_id		int		not null,
  bm_universe_id	int		not null,
  account_cd		varchar(32)	not null,
  representative	bit		null
)
go
