use QER
go

create table dbo.access_group_def (
  group_id		int	identity(1,1)	NOT NULL,
  group_nm		varchar(128)	NULL
)
go
