use QER
go

create table dbo.scores (
  bdate			datetime	not null,
  strategy_id		int		not null,
  mqa_id		varchar(32)	null,
  ticker		varchar(16)	null,
  cusip			varchar(32)	null,
  sedol			varchar(32)	null,
  isin			varchar(64)	null,
  gv_key		int		null,
  sector_score		float		null,
  segment_score		float		null,
  ss_score		float		null,
  universe_score	float		null,
  total_score		float		null
)
go

CREATE CLUSTERED INDEX IX_scores ON scores (bdate, strategy_id)
go
