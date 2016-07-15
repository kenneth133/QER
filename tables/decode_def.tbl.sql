use QER
go

create table dbo.decode_def (
  item		varchar(64)	not null	primary key,
  [description]	varchar(255)	null
)
go
