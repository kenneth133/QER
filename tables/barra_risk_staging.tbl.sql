use QER
go

create table dbo.barra_risk_staging (
  BARRID		varchar(16)	not null,
  TICKER		varchar(16)	null,
  CUSIP			varchar(32)	null,
  NAME			varchar(64)	null,
  HBTA			float		null,
  BETA			float		null,
  SRISK_PCT		float		null,
  TRISK_PCT		float		null,
  VOLTILTY		float		null,
  MOMENTUM		float		null,
  SIZE			float		null,
  SIZENONL		float		null,
  TRADEACT		float		null,
  GROWTH		float		null,
  EARNYLD		float		null,
  VALUE			float		null,
  EARNVAR		float		null,
  LEVERAGE		float		null,
  CURRSEN		float		null,
  YIELD			float		null,
  NONESTU		bit		null,
  INDNAME1		varchar(32)	null,
  IND1			int		null,
  WGT1_PCT		float		null,
  INDNAME2		varchar(32)	null,
  IND2			int		null,
  WGT2_PCT		float		null,
  INDNAME3		varchar(32)	null,
  IND3			int		null,
  WGT3_PCT		float		null,
  INDNAME4		varchar(32)	null,
  IND4			int		null,
  WGT4_PCT		float		null,
  INDNAME5		varchar(32)	null,
  IND5			int		null,
  WGT5_PCT		float		null,
  PRICE			float		null,
  CAPITALIZATION	float		null,
  YLD_PCT		float		null,
  SAP500		bit		null,
  SAPVAL		bit		null,
  SAPGRO		bit		null,
  MIDCAP		bit		null,
  MIDVAL		bit		null,
  MIDGRO		bit		null,
  SC600			bit		null,
  SCVAL			bit		null,
  SCGRO			bit		null,
  E3ESTU		bit		null
)
go

/* steps for loading barra_risk table from this staging table:
1. truncate staging table
2. bcp from file to staging table
3. prune for new industry definitions
4. copy to permanent table with month_end_dt
     perform boolean conversions
*/