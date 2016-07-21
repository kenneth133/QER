use QER
go

IF OBJECT_ID('dbo.instrument_factor_staging') IS NOT NULL
  BEGIN drop table dbo.instrument_factor_staging END
go

create table dbo.instrument_factor_staging (
  bdate			datetime	null,

  mqa_id		varchar(32)	null,
  ticker		varchar(32)	null,
  cusip			varchar(32)	null,
  sedol			varchar(32)	null,
  isin			varchar(32)	null,
  currency_cd	varchar(3)	null,
  exchange_nm	varchar(60)	null,

  factor_cd		varchar(64)	null,
  factor_value	float		null
)
go

CREATE NONCLUSTERED INDEX IX_instrument_factor_staging ON instrument_factor_staging (bdate, factor_cd, cusip)
go
