use QER
go

create table dbo.us_high_dividend_staging (
  gics_sector_nm		varchar(64)		null,
  gics_industry_nm		varchar(64)		null,
  ticker				varchar(32)		null,
  company_nm			varchar(100)	null,
  sedol					varchar(32)		null,
  cusip					varchar(32)		null,
  mkt_cap				float			null,
  div_yield				float			null,
  dps_growth			float			null,
  div_payout_ltm		float			null,
  debt_to_capital		float			null,
  interest_coverage		float			null,
  fcf_ltm_to_div_ltm	float			null,
  div_yield_to_5yr_avg	float			null,
  pb_to_5yr_avg			float			null,
  sp_current_rating		varchar(4)		null,
  sp_senior_rating		varchar(4)		null
)
go
