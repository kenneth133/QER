use QER
go

create table dbo.position (
  bdate		datetime	NOT NULL,
  account_cd	varchar(32)	NOT NULL,
  mqa_id	varchar(32)	NULL,
  ticker	varchar(16)	NULL,
  cusip		varchar(32)	NULL,
  sedol		varchar(32)	NULL,
  isin		varchar(64)	NULL,
  gv_key	int		NULL,
  units		float		NOT NULL
)
go

CREATE CLUSTERED INDEX IX_position ON position (bdate, account_cd)
go
