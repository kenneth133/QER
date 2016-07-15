use QER
go

create table dbo.weight_factor_sector (
  weight_model_id	int		not null,--key
  factor_id		int		not null,--key
  sector_id		int		not null,--key
  sector_wgt		float		null
)
go