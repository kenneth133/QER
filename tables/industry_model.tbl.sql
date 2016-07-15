use QER
go

create table dbo.industry_model (
  industry_model_id	int id(1,1)	not null	primary key,
  industry_model_cd	varchar(16)	not null,
  industry_model_nm	varchar(64)	not null
)
go
