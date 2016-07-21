use QER
go

create table dbo.region_model (
  region_model_id	int identity(1,1)	not null,
  region_model_nm	varchar(128)		not null
)
go
