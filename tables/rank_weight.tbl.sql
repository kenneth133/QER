use QER
go

create table dbo.rank_weight (
  rank_wgt_id	int	not null,
  period_back	int	not null,
  period_wgt	float	null
)
go
