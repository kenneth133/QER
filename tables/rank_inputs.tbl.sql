use QER
go

create table dbo.rank_inputs (
  rank_event_id		int identity(1,1)	not null	primary key,
  run_tm		datetime		not null,
  as_of_date		datetime		not null,
  bdate			datetime		not null,
  universe_dt		datetime		not null,
  universe_id		int			not null,
  factor_id		int			not null,
  factor_source_cd	varchar(8)		null,
  groups		int			not null,
  against		varchar(1)		not null,
  against_id		int			null,
  rank_wgt_id		int			null,
  period_type		varchar(1)		null,
  method		varchar(4)		not null,
  missing_method	varchar(8)		not null,
  missing_value		float			null
)
go

CREATE NONCLUSTERED INDEX IX_rank_inputs ON rank_inputs (bdate, factor_id)
go
