use QER
go

create table dbo.mqa_id_staging (
  bdate			datetime	null,
  mqa_id		varchar(32)	null,
  cusip_input	varchar(32)	null,
  sedol_input	varchar(32)	null
)
go
