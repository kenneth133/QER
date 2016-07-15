
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[FK_Report_Column_Setting_Report_Column]') and OBJECTPROPERTY(id, N'IsForeignKey') = 1)
ALTER TABLE [dbo].[Report_Column_Setting] DROP CONSTRAINT FK_Report_Column_Setting_Report_Column
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[Report_Column]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
drop table [dbo].[Report_Column]
GO

CREATE TABLE [dbo].[Report_Column] (
	[Report_Id] [int] NOT NULL ,
	[Column_Num] [int] NOT NULL ,
	[Report_nm] [varchar] (64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[Column_nm] [varchar] (64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL ,
	[Column_Display_nm] [varchar] (100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[Column_Group_Num] [int] NULL ,
	[Column_Group_nm] [varchar] (64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[Column_Width] [int] NOT NULL ,
	[Column_Align] [varchar] (8) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[Column_Format] [varchar] (10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL 
) ON [PRIMARY]
GO

