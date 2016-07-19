use QER
go

create table dbo.category_score (
  bdate			datetime		NOT NULL,
  strategy_id	int				NOT NULL,
  security_id	int				NOT NULL,
  score_level	varchar(1)		NOT NULL,
  category		varchar(1)		NOT NULL,
  category_score float			NULL
)

CREATE CLUSTERED INDEX IX_category_score ON category_score (bdate, strategy_id)
go
