use QER
go

create table dbo.segment_def (
  sector_id	int		not null,
  segment_id	int id(1,1)	not null	primary key,
  segment_num	int		not null,
  segment_nm	varchar(128)	null
)
go
