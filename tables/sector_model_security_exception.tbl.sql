use QER
go

create table dbo.sector_model_security_exception (
  override		bit		not null,

  sector_model_id	int		null,
  sector_id		int		null,
  segment_id		int		null,

  mqa_id		varchar(32)	null,
  ticker		varchar(16)	null,
  cusip			varchar(32)	null,
  sedol			varchar(32)	null,
  isin			varchar(64)	null,
  gv_key		int		null
)
go
