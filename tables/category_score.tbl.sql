use QER
go

create table dbo.category_score (
  bdate			datetime		NOT NULL,
  strategy_id	int				NOT NULL,
  mqa_id		varchar(32)		NULL,
  ticker		varchar(16)		NULL,
  cusip			varchar(32)		NULL,
  sedol			varchar(32)		NULL,
  isin			varchar(64)		NULL,
  gv_key		int				NULL,
  score_level	varchar(1)		NOT NULL,
  category		varchar(1)		NOT NULL,
  category_score float			NULL
)
go

CREATE CLUSTERED INDEX IX_category_score ON category_score (bdate, strategy_id)
go
