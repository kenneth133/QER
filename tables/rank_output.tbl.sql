use QER
go

create table dbo.rank_output (
  rank_event_id		int		not null,
  mqa_id		varchar(32)	null,
  ticker		varchar(16)	null,
  cusip			varchar(32)	null,
  sedol			varchar(32)	null,
  isin			varchar(64)	null,
  gv_key		int		null,
  factor_value		float		null,
  rank			int		null
)
go

CREATE CLUSTERED INDEX IX_rank_output ON rank_output (rank_event_id)
go
