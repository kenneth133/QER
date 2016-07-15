use QER
go

create table dbo.factor_segment (
  factor_model_id	int	not null,--key
  factor_id		int	not null,--key
  segment_id		int	not null --key
)
go
