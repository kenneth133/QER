use QER
go

IF OBJECT_ID('dbo.universe_factset_staging') IS NOT NULL
  BEGIN drop table dbo.universe_factset_staging END
go

create table dbo.universe_factset_staging (
  universe_dt		datetime	null,
  mqa_id			varchar(32)	null,
  ticker			varchar(32)	null,
  cusip				varchar(32)	null,
  sedol				varchar(32)	null,
  isin				varchar(32)	null,
  currency_cd		varchar(3)	null,
  exchange_nm		varchar(60)	null,
  gics_sub_industry_num	int		null,
  russell_sector_num	int		null,
  russell_industry_num	int		null
)
go

CREATE NONCLUSTERED INDEX IX_universe_factset_staging ON universe_factset_staging (universe_dt, cusip)
go
