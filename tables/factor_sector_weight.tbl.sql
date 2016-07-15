use QER
go

create table dbo.factor_sector_weight (
  factor_model_id	int	not null,--key
  factor_id		int	not null,--key
  sector_id		int	not null,--key
  sector_wgt		float	null
)
go
