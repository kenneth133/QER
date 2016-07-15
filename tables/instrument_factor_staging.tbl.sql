use QER
go

create table dbo.instrument_factor_staging (
  bdate			datetime	null,

  mqa_id		varchar(32)	null,
  ticker		varchar(16)	null,
  cusip			varchar(32)	null,
  sedol			varchar(32)	null,
  isin			varchar(64)	null,
  gv_key		int		null,

  factor_cd		varchar(64)	null,
  factor_value		float		null
)
go

CREATE NONCLUSTERED INDEX IX_instrument_factor_staging ON instrument_factor_staging (bdate, factor_cd, cusip)
go
