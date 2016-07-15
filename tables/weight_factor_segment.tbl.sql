use QER
go

create table dbo.weight_factor_segment (
  weight_model_id	int	not null,--key
  factor_id		int	not null,--key
  segment_id		int	not null,--key
  segment_wgt		float	null
)
go