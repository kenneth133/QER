use QER
go

create table dbo.access_group_makeup (
  group_id		int			NOT	NULL,
  member_type	varchar(1)	NOT NULL,
  member_id		int			NOT NULL
)
go
