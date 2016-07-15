use QER
go

create table dbo.account_sleeve (
  account_parent	varchar(32)	not null,
  account_child		varchar(32)	not null,
  primary key (account_parent, account_child)
)
go
