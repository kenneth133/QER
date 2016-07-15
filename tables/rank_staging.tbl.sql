use QER
go

create table dbo.rank_staging (
  bdate			datetime		NULL,
  universe_dt	datetime		NULL,
  mqa_id		varchar(32)		NULL,
  ticker		varchar(16)		NULL,
  cusip			varchar(32)		NULL,
  sedol			varchar(32)		NULL,
  isin			varchar(64)		NULL,
  gv_key		int				NULL,
  factor_value	float			NULL,
  rank			int				NULL
)
go
