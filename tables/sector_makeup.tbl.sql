use QER
go

create table dbo.sector_makeup (
  sector_id			int		not null,
  sector_child_type		varchar(1)	not null,--C=sector, G=segment, I=industry, B=subindustry
  sector_child_id		int		not null,
  primary key (sector_id, sector_child_type, sector_child_id)
)
go
