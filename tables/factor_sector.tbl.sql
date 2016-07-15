use QER
go

create table dbo.factor_sector (
  factor_model_id	int	not null,--key
  factor_id		int	not null,--key
  sector_id		int	not null --key
)
go
