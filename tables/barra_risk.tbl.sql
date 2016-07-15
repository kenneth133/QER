use QER
go

create table dbo.barra_risk (
  month_end_dt		datetime	not null,
  barra_id		varchar(16)	not null,
  ticker		varchar(16)	null,
  cusip			varchar(32)	null,
  name			varchar(64)	null,
  hist_beta		float		null,
  beta			float		null,
  spec_risk		float		null,		-- convert %
  tot_risk		float		null,		-- convert %
  volatility		float		null,
  momentum		float		null,
  size			float		null,
  size_nonlin		float		null,
  trade_act		float		null,
  growth		float		null,
  earn_yield		float		null,
  value			float		null,
  earn_var		float		null,
  leverage		float		null,
  curr_sen		float		null,
  dividend_yield	float		null,
  in_non_est_univ	bit		null,
  ind_cd1		int		null,
  wgt1			float		null,		-- convert %
  ind_cd2		int		null,
  wgt2			float		null,		-- convert %
  ind_cd3		int		null,
  wgt3			float		null,		-- convert %
  ind_cd4		int		null,
  wgt4			float		null,		-- convert %
  ind_cd5		int		null,
  wgt5			float		null,		-- convert %
  price			float		null,
  capitalization	float		null,
  yield			float		null,		-- convert %
  in_SAP500		bit		null,
  in_SAPVAL		bit		null,
  in_SAPGRO		bit		null,
  in_MIDCAP		bit		null,
  in_MIDVAL		bit		null,
  in_MIDGRO		bit		null,
  in_SC600		bit		null,
  in_SCVAL		bit		null,
  in_SCGRO		bit		null,
  in_E3ESTU		bit		null,
  primary key (month_end_dt, barra_id)
)
go

CREATE NONCLUSTERED INDEX IX_barra_risk ON barra_risk (month_end_dt, cusip)
go
