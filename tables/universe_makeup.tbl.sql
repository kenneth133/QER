use QER
go

create table dbo.universe_makeup (
  universe_dt	datetime	not null,
  universe_id	int			not null,
  security_id	int			not null,
  weight		float		null
)
go

CREATE CLUSTERED INDEX IX_universe_makeup ON universe_makeup (universe_dt, universe_id, security_id)
go
