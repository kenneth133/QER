use QER
go

create table dbo.instrument_factor (
  bdate			datetime	not null,

  mqa_id		varchar(32)	null,
  ticker		varchar(16)	null,
  cusip			varchar(32)	null,
  sedol			varchar(32)	null,
  isin			varchar(64)	null,
  gv_key		int		null,

  factor_id		int		not null,
  factor_value		float		null,

  update_tm		datetime	not null,
  source_cd		varchar(8)	not null
)
go

CREATE CLUSTERED INDEX IX_instrument_factor_1 ON instrument_factor (bdate, factor_id, cusip)
CREATE NONCLUSTERED INDEX IX_instrument_factor_2 ON instrument_factor (bdate, factor_id, mqa_id)
go
