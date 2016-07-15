use QER
go

create table dbo.universe_makeup (
  universe_dt	datetime	not null,
  universe_id	int		not null,
  mqa_id	varchar(32)	null,
  ticker	varchar(16)	null,
  cusip		varchar(32)	null,
  sedol		varchar(32)	null,
  isin		varchar(64)	null,
  gv_key	int		null,
  weight	float		null
)
go

CREATE CLUSTERED INDEX IX_universe_makeup_1 ON universe_makeup (universe_dt, universe_id, cusip)
CREATE NONCLUSTERED INDEX IX_universe_makeup_2 ON universe_makeup (universe_dt, universe_id, mqa_id)
go
