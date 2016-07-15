use QER
go

create table dbo.sector_model (
  sector_model_id		int id(1,1)	not null	primary key,
  sector_model_cd		varchar(32)	not null,
  sector_model_nm		varchar(64)	not null
)
go
