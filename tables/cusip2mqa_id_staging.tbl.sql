use QER
go

create table dbo.cusip2mqa_id_staging (
  bdate		datetime	null,
  mqa_id	varchar(32)	null,
  input_cusip	varchar(32)	null,
  mqa_cusip	varchar(32)	null,
  mqa_ticker	varchar(16)	null,
  mqa_gv_key	int		null
)
go

CREATE NONCLUSTERED INDEX IX_cusip2mqa_id_staging_1 ON cusip2mqa_id_staging (bdate, input_cusip)
CREATE NONCLUSTERED INDEX IX_cusip2mqa_id_staging_2 ON cusip2mqa_id_staging (bdate, mqa_cusip)
go
