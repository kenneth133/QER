use QER
go

create table dbo.industry (
  industry_model_id	int		not null,
  industry_id		int id(1,1)	not null,
  industry_num		int		not null,
  industry_nm		varchar(64)	null,
  primary key (industry_model_id, industry_id)
)
go
