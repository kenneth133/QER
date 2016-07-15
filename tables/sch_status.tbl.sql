use QER
go

create table dbo.sch_status (
  job_name	varchar(64)	not null,
  prev_bus_day	datetime	not null,
  started	datetime	not null,
  ended		datetime	null,
  status	varchar(32)	not null
)
go
