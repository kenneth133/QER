use QER
go

create table dbo.lcr_scores (
  priced_date		datetime	not null,
  mandate_id		int		null,
  sector_id		int		not null,
  ticker		varchar(16)	not null,
  sector_score		float		null,
  segment_score		float		null,
  portfolio_score	float		null
)
go
