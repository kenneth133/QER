use QER
go

create table dbo.factor_universe_weight (
  factor_model_id	int		not null,--key
  factor_id		int		not null,--key
  universe_wgt		float		null
)
go
