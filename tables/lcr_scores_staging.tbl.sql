use QER
go

create table dbo.lcr_scores_staging (
  ticker		varchar(16)	not null,
  sector_score		float		null,
  segment_score		float		null,
  portfolio_score	float		null
)
go
