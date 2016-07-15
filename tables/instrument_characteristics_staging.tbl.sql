use QER
go

create table dbo.instrument_characteristics_staging (
  bdate			datetime	null,

  mqa_id		varchar(32)	null,
  ticker		varchar(16)	null,
  cusip			varchar(32)	null,
  sedol			varchar(32)	null,
  isin			varchar(64)	null,
  gv_key		int		null,
  imnt_nm		varchar(255)	null,

  price_to_book		float		null,
  price_close		float		null,
  price_close_local	float		null,

  currency_local	varchar(8)	null,
  currency_nm		varchar(128)	null,
  exchange		varchar(4)	null,
  dexchange		varchar(128)	null,
  country		varchar(8)	null,
  ctry_name		varchar(128)	null,
  sectype		varchar(4)	null,
  dsectype		varchar(128)	null,

  mktcap		float		null,
  volume		float		null,
  volatility		float		null,
  beta			float		null,
  quality		int		null,

  gics_sector_num	int		null,
  gics_sector_nm	varchar(64)	null,
  gics_segment_num	int		null,
  gics_segment_nm	varchar(128)	null,
  gics_industry_num	int		null,
  gics_industry_nm	varchar(255)	null,
  gics_sub_industry_num	int		null,
  gics_sub_industry_nm	varchar(255)	null,

  russell_sector_num	int		null,
  russell_sector_nm	varchar(64)	null,
  russell_industry_num	int		null,
  russell_industry_nm	varchar(255)	null
)
go

CREATE NONCLUSTERED INDEX IX_instrument_characteristics_staging ON instrument_characteristics_staging (bdate, cusip)
go
