use QER
go

create table dbo.sector_model_security_exception (
  override		bit		not null,
  sector_model_id int	null,
  sector_id		int		null,
  segment_id	int		null,
  security_id	int		not null
)
go
