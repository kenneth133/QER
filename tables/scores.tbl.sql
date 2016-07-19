use QER
go

create table dbo.scores (
  bdate			datetime	not null,
  strategy_id	int			not null,
  security_id	int			not null,
  sector_score		float		null,
  segment_score		float		null,
  ss_score			float		null,
  universe_score	float		null,
  country_score		float		null,
  total_score		float		null
)

CREATE CLUSTERED INDEX IX_scores ON scores (bdate, strategy_id)
go
