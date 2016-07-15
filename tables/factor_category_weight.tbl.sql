use QER
go

create table dbo.factor_category_weight (
  factor_model_id	int		not null,--key
  factor_id		int		not null,--key
  category		varchar(1)	null,
  category_wgt		float		null
)
go
