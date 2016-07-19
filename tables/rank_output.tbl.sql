use QER
go

create table dbo.rank_output (
  rank_event_id		int		not null,
  security_id		int		not null,
  factor_value		float	null,
  rank				int		null
)
go

CREATE CLUSTERED INDEX IX_rank_output ON rank_output (rank_event_id)
go
