use QER
go

create table dbo.segment_makeup (
  segment_id		int		not null,
  segment_child_type	varchar(1)	not null,--C=sector, G=segment, I=industry, B=subindustry
  segment_child_id	int		not null,
  primary key (segment_id, segment_child_type, segment_child_id)
)
go
