use QER
go

create table dbo.factor_model_weights (
  factor_model_id	int		not null,
  sector_id		int		null,
  segment_id		int		null,
  sector_ss_wgt		float		null,
  segment_ss_wgt	float		null,
  ss_total_wgt		float		null,
  universe_total_wgt	float		null,
  country_total_wgt	float		null,
  region_total_wgt	float		null
)
go

CREATE CLUSTERED INDEX IX_factor_model_weights ON factor_model_weights (factor_model_id, sector_id, segment_id)
go
