use QER
go

create table dbo.weight_total (
  weight_model_id	int		not null,
  sector_id		int		not null,
  segment_id		int		null,
  sector_ss_wgt		float		null,
  segment_ss_wgt	float		null,
  ss_total_wgt		float		null,
  universe_total_wgt	float		null
)
go
