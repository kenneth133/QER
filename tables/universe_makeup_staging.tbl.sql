use QER
go

create table dbo.universe_makeup_staging (
  ID		varchar(32)	null,
  universe_dt	datetime	not null,
  universe_cd	varchar(32)	not null,
  mqa_ticker	varchar(16)	null,
  mqa_id	varchar(32)	null,
  ticker	varchar(16)	null,
  cusip		varchar(32)	null,
  sedol		varchar(32)	null,
  isin		varchar(64)	null,
  gv_key	int		null,
  weight	float		null
)
go

CREATE NONCLUSTERED INDEX IX_universe_makeup_staging_1 ON universe_makeup_staging (universe_dt, cusip)
CREATE NONCLUSTERED INDEX IX_universe_makeup_staging_2 ON universe_makeup_staging (universe_dt, universe_cd, cusip)
go
