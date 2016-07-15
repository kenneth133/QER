use QER
go

create table dbo.sector_def (
  sector_model_id	int		not null,
  sector_id		int id(1,1)	not null	primary key,
  sector_num		int		not null,
  sector_nm		varchar(64)	null
)
go
