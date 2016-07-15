use QER
go

create table dbo.sub_industry (
  industry_id		int		not null,
  sub_industry_num	int		not null,
  sub_industry_nm	varchar(64)	null,
  primary key (industry_id, sub_industry_num)
)
go
