use QER
go

create table dbo.factor_against_weight_override (
  factor_model_id	int		not null,
  factor_id		int		null,
  level_type		varchar(1)	null,
  level_id		int		null,
  against		varchar(1)	null,
  against_id		int		null,
  override_wgt		float		null
)
go
