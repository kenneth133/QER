use QER
go

create table dbo.factor_return_output (
  factor_return_event_id	int	not null,
  xile				int	not null,
  eq_return			float	not null,
  cap_return			float	not null
)
go
