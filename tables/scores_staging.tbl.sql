use QER
go

create table dbo.scores_staging (
  bdate			datetime		NULL,
  strategy_cd	varchar(16)		NULL,
  mqa_id		varchar(32)		NULL,
  ticker		varchar(16)		NULL,
  cusip			varchar(32)		NULL,
  sedol			varchar(32)		NULL,
  isin			varchar(64)		NULL,
  gv_key		int				NULL,
  sector_score	float			NULL,
  segment_score	float			NULL,
  ss_score		float			NULL,
  universe_score	float		NULL,
  country_score	float			NULL,
  total_score	float			NULL
)
go
