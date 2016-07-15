use QER
go

create table dbo.instrument_characteristics (
  bdate			datetime	not null,

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
  exchange		varchar(4)	null,
  country		varchar(8)	null,
  sec_type		varchar(4)	null,

  mkt_cap		float		null,
  volume		float		null,
  volume30		float		null,
  volume60		float		null,
  volume90		float		null,
  volatility		float		null,
  beta			float		null,
  quality		int		null,

  gics_sector_num	int		null,
  gics_segment_num	int		null,
  gics_industry_num	int		null,
  gics_sub_industry_num	int		null,

  russell_sector_num	int		null,
  russell_industry_num	int		null,

  update_tm		datetime	not null,
  source_cd		varchar(8)	not null
)
go

CREATE CLUSTERED INDEX IX_instrument_characteristics_1 ON instrument_characteristics (bdate, cusip)
CREATE NONCLUSTERED INDEX IX_instrument_characteristics_2 ON instrument_characteristics (bdate, mqa_id)
go
