use QER
go

create table dbo.factor_model (
  factor_model_id	int identity(1,1)	not null	primary key,
  factor_model_cd	varchar(16)		not null,
  factor_model_nm	varchar(64)		not null,
  sector_model_id	int			not null
)
go
