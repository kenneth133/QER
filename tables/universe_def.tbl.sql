use QER
go

create table dbo.universe_def (
  universe_id		int identity(1,1) not null	primary key,
  universe_cd		varchar(32)	not null,
  mqa_ticker		varchar(16)	null,
  mqa_cusip		varchar(16)	null,
  mqa_name		varchar(128)	null,
  universe_nm		varchar(128)	not null
)
go
