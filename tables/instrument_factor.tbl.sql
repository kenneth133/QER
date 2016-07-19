use QER
go

create table dbo.instrument_factor (
  bdate			datetime	not null,
  security_id	int			not null,

  factor_id		int			not null,
  factor_value	float		null,

  update_tm		datetime	not null,
  source_cd		varchar(8)	not null
)
go

CREATE CLUSTERED INDEX IX_instrument_factor ON instrument_factor (bdate, factor_id, security_id)
go
