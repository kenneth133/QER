use QER
go

create table dbo.weight_model (
  weight_model_id	int		not null,--key id
  weight_model_cd	varchar(16)	not null,
  weight_model_nm	varchar(128)	not null,
  factor_model_id	int		not null,
)
go
