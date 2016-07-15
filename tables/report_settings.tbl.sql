use QER
go

create table dbo.report_settings (
  report_nm		varchar(64)	NOT NULL,
  username		varchar(64)	NOT NULL,
  column_group_num	int		NULL,
  column_group_nm	varchar(64)	NULL,
  column_num		int		NOT NULL,
  column_nm		varchar(64)	NOT NULL,
  visible		bit		NOT NULL
)
go
