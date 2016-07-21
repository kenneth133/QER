use QER
go

IF OBJECT_ID('dbo.scores_staging') IS NOT NULL
  BEGIN drop table dbo.scores_staging END
go

create table dbo.scores_staging (
  bdate			datetime		NULL,
  strategy_cd	varchar(16)		NULL,

  mqa_id		varchar(32)		NULL,
  ticker		varchar(16)		NULL,
  cusip			varchar(32)		NULL,
  sedol			varchar(32)		NULL,
  isin			varchar(64)		NULL,
  currency_cd	varchar(3)		NULL,
  exchange_nm	varchar(60)		NULL,

  sector_score	float			NULL,
  segment_score	float			NULL,
  ss_score		float			NULL,
  universe_score float			NULL,
  country_score	float			NULL,
  region_score	float			NULL,
  total_score	float			NULL
)
go
