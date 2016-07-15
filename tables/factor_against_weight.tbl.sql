use QER
go

create table dbo.factor_against_weight (
  factor_model_id	int		NOT NULL,
  factor_id		int		NOT NULL,
  against		varchar(1)	NOT NULL,
  against_id		int		NULL,
  weight		float		NULL
)
go

CREATE NONCLUSTERED INDEX IX_factor_against_weight_1 ON factor_against_weight (factor_model_id, factor_id, against)
CREATE NONCLUSTERED INDEX IX_factor_against_weight_2 ON factor_against_weight (factor_model_id, against)
go
