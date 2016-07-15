use QER
go

create table dbo.factor_return_inputs (
  factor_return_event_id	int identity(1,1)	not null,
  run_tm			datetime		not null,
  as_of_date			datetime		not null,
  bdate				datetime		not null,
  universe_dt			datetime		not null,
  universe_id			int			not null,
  factor_id			int			not null,
  factor_source_cd		varchar(8)		null,

  return_factor_id		int			not null,
  return_factor_source_cd	varchar(8)		null,

  groups			int			not null,
  sector_model_id		int			null,
  against			varchar(1)		not null,
  against_num			int			null,
  rank_wgt_id			int			null,
  period_type			varchar(1)		null,
  method			varchar(4)		not null,
  missing_method		varchar(8)		not null,
  missing_value			float			null
)
go
