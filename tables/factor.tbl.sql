use QER
go

create table dbo.factor (
  factor_id		int identity(1,1)	not null	primary key,
  factor_cd		varchar(32)		not null,
  factor_short_nm	varchar(64)		not null,
  factor_nm		varchar(255)		null
)
go
