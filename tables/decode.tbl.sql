use QER
go

create table dbo.decode (
  item		varchar(64)		not null,
  code		varchar(64)		null,
  decode	varchar(255)		null
)
go

CREATE CLUSTERED INDEX IX_decode ON decode (item, code)
go
