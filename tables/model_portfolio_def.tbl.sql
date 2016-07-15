use QER
go

create table dbo.model_portfolio_def (
  model_portfolio_def_id	int identity(1,1)	NOT NULL,
  model_portfolio_def_cd	varchar(32)		NOT NULL,
  model_portfolio_def_nm	varchar(255)		NULL
)
go
