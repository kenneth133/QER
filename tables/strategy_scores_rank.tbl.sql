use QER
go

create table dbo.strategy_scores_rank (
  strategy_id			int	not null,
  sector_score_fractile		int	null,
  segment_score_fractile	int	null,
  ss_score_fractile		int	null,
  universe_score_fractile	int	null,
  total_score_fractile		int	null
)
go
