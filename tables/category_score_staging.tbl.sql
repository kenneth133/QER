use QER
go

create table dbo.category_score_staging (
  bdate			datetime		NULL,
  strategy_cd	varchar(16)		NULL,
  mqa_id		varchar(32)		NULL,
  ticker		varchar(16)		NULL,
  cusip			varchar(32)		NULL,
  sedol			varchar(32)		NULL,
  isin			varchar(64)		NULL,
  gv_key		int				NULL,
  score_level	varchar(32)		NULL,
  category_nm	varchar(64)		NULL,
  category_score float			NULL
)
go
