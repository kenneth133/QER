use QER
go

create table dbo.return_calc_result (
  return_calc_id	int		not null,
  univ_type		varchar(32)	not null,
  sector_model_id	int		null,
  sector_id		int		null,
  segment_id		int		null,
  rtn			float		not null
)
go

CREATE CLUSTERED INDEX IX_return_calc_result ON return_calc_result (return_calc_id)
go
