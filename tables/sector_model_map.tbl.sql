use QER
go

create table dbo.sector_model_map (
  map_id			int not null,
  sector_model_id	int	not null,
  sector_id			int	null,
  segment_id		int null,

  gics_sector_num	int	null,
  gics_segment_num	int	null,
  gics_industry_num	int	null,
  gics_sub_industry_num	int	null,

  russell_sector_num	int	null,
  russell_industry_num	int	null
)
go

create clustered index IX_sector_model_map on sector_model_map (sector_model_id, map_id)
go
