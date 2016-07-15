use QER
go

create table dbo.holiday (
  schedule	varchar(16)		not null,
  date		datetime		not null,
  description	varchar(255)		null,
  primary key (schedule, date)
)
go
