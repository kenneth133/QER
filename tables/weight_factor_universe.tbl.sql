use QER
go

create table dbo.weight_factor_universe (
  weight_model_id	int	not null,--key
  factor_id		int	not null,--key
  universe_wgt		float	null
)
go