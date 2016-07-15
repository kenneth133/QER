use QER
go

create table dbo.sector_model_security (
  bdate			datetime	not null,
  sector_model_id	int		not null,

  sector_id		int		null,
  segment_id		int		null,

  mqa_id		varchar(32)	null,
  ticker		varchar(16)	null,
  cusip			varchar(32)	null,
  sedol			varchar(32)	null,
  isin			varchar(64)	null,
  gv_key		int		null
)
go

CREATE NONCLUSTERED INDEX IX_sector_model_security_1 ON sector_model_security (bdate, sector_model_id, cusip)
CREATE NONCLUSTERED INDEX IX_sector_model_security_2 ON sector_model_security (bdate, sector_model_id, sector_id)
go
