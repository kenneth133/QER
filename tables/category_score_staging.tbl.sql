use QER
go

IF OBJECT_ID('dbo.category_score_staging') IS NOT NULL
  BEGIN drop table dbo.category_score_staging END
go

create table dbo.category_score_staging (
  bdate			datetime		NULL,
  strategy_cd	varchar(16)		NULL,

  mqa_id		varchar(32)		NULL,
  ticker		varchar(32)		NULL,
  cusip			varchar(32)		NULL,
  sedol			varchar(32)		NULL,
  isin			varchar(32)		NULL,
  currency_cd	varchar(3)		NULL,
  exchange_nm	varchar(60)		NULL,

  score_level	varchar(32)		NULL,
  category_nm	varchar(64)		NULL,
  category_score float			NULL
)
go
