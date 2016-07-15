use QER
go

create table dbo.lcr_scores_map (
  sector_id		int		null,
  filename		varchar(64)	not null,
  ticker		int		null,
  sector_score		int		null,
  segment_score		int		null,
  portfolio_score	int		null
)
go
