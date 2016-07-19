use QER
go

IF OBJECT_ID('dbo.universe_makeup_staging') IS NOT NULL
  BEGIN drop table dbo.universe_makeup_staging END
go

create table dbo.universe_makeup_staging (
  universe_dt	datetime	not null,
  universe_cd	varchar(32)	not null,

  mqa_id		varchar(32)	null,
  ticker		varchar(16)	null,
  cusip			varchar(32)	null,
  sedol			varchar(32)	null,
  isin			varchar(64)	null,
  currency_cd	varchar(3)	null,
  exchange_nm	varchar(40)	null,

  weight		float		null
)
go

CREATE NONCLUSTERED INDEX IX_universe_makeup_staging_1 ON universe_makeup_staging (universe_dt, cusip)
CREATE NONCLUSTERED INDEX IX_universe_makeup_staging_2 ON universe_makeup_staging (universe_dt, universe_cd, cusip)
go
