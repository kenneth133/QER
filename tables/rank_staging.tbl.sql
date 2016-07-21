use QER
go

IF OBJECT_ID('dbo.rank_staging') IS NOT NULL
  BEGIN drop table dbo.rank_staging END
go

create table dbo.rank_staging (
  bdate			datetime		NULL,
  universe_dt	datetime		NULL,

  mqa_id		varchar(32)		NULL,
  ticker		varchar(32)		NULL,
  cusip			varchar(32)		NULL,
  sedol			varchar(32)		NULL,
  isin			varchar(32)		NULL,
  currency_cd	varchar(3)		NULL,
  exchange_nm	varchar(60)		NULL,

  factor_value	float			NULL,
  rank			int				NULL
)
go
