
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[Report_Column_Setting]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
drop table [dbo].[Report_Column_Setting]
GO

CREATE TABLE [dbo].[Report_Column_Setting] (
	[Report_Id] [int] NOT NULL ,
	[Column_num] [int] NOT NULL ,
	[Username] [varchar] (64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL 
) ON [PRIMARY]
GO

