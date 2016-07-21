use QER
go

create table dbo.region_def (
  region_model_id	int					not null,
  region_id			int identity(1,1)	not null,
  region_nm			varchar(128)		not null
)
go
