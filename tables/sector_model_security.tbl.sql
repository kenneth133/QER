use QER
go

create table dbo.sector_model_security (
  bdate			datetime	not null,
  sector_model_id int		not null,
  sector_id		int			null,
  segment_id	int			null,
  security_id	int			not null
)

CREATE NONCLUSTERED INDEX IX_sector_model_security_1 ON sector_model_security (bdate, sector_model_id, security_id)
CREATE NONCLUSTERED INDEX IX_sector_model_security_2 ON sector_model_security (bdate, sector_model_id, sector_id)
go
