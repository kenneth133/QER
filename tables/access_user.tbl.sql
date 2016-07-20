use QER
go

CREATE TABLE dbo.access_user (
  user_id	int identity(1,1) not null,
  user_nm	varchar(32)	null
)
go
