use QER
go

create table dbo.cusip2mqa_id (
  bdate		datetime	null,
  mqa_id	varchar(32)	null,
  input_cusip	varchar(32)	null,
  mqa_cusip	varchar(32)	null,
  mqa_ticker	varchar(16)	null,
  mqa_gv_key	int		null
)
go
