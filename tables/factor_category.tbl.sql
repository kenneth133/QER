use QER
go

create table dbo.factor_category (
  factor_model_id	int		not null,
  factor_id		int		not null,
  category		varchar(1)	null,
  primary key (factor_model_id, factor_id)
)
go
