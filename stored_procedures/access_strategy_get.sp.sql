USE [QER]
GO

CREATE TABLE #STRATEGY_ID ( strategy_id int NOT NULL )

IF OBJECT_ID('dbo.access_strategy_get') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.access_strategy_get
    IF OBJECT_ID('dbo.access_strategy_get') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.access_strategy_get >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.access_strategy_get >>>'
END
go
CREATE PROCEDURE [dbo].[access_strategy_get]
@USERID int = NULL,
@DEBUG bit = NULL
AS

DECLARE @USER_ID int

IF @USERID IS NULL
BEGIN
  SELECT @USER_ID = [user_id]
    FROM access_user
   WHERE user_nm = system_user
END
ELSE
  BEGIN SELECT @USER_ID = @USERID END

IF @DEBUG = 1
BEGIN
  SELECT '@USER_ID', @USER_ID
  SELECT * FROM access_user WHERE [user_id] = @USER_ID
END

CREATE TABLE #GROUP ( group_id int NOT NULL )

INSERT #GROUP
SELECT group_id
  FROM access_group_makeup
 WHERE member_type = 'U'
   AND member_id = @USER_ID

IF @DEBUG = 1
BEGIN
  SELECT '#GROUP'
  SELECT * FROM #GROUP ORDER BY group_id
END

WHILE EXISTS (SELECT * FROM access_group_makeup
               WHERE member_type = 'G'
                 AND member_id IN (SELECT group_id FROM #GROUP)
                 AND group_id NOT IN (SELECT group_id FROM #GROUP))
BEGIN
  INSERT #GROUP
  SELECT group_id
    FROM access_group_makeup
   WHERE member_type = 'G'
     AND member_id IN (SELECT group_id FROM #GROUP)
     AND group_id NOT IN (SELECT group_id FROM #GROUP)

  IF @DEBUG = 1
  BEGIN
    SELECT '#GROUP'
    SELECT * FROM #GROUP ORDER BY group_id
  END
END

IF EXISTS (SELECT * FROM #GROUP WHERE group_id IN (SELECT group_id FROM access_group_def WHERE group_nm = 'SUPER USER'))
  BEGIN INSERT #STRATEGY_ID SELECT strategy_id FROM strategy END
ELSE
BEGIN
  INSERT #STRATEGY_ID
  SELECT strategy_id FROM access_strategy
   WHERE member_type = 'U' AND member_id = @USER_ID
  UNION
  SELECT strategy_id FROM access_strategy
   WHERE member_type = 'G' AND member_id IN (SELECT group_id FROM #GROUP)
END

IF @DEBUG = 1
BEGIN
  SELECT '#STRATEGY_ID'
  SELECT * FROM #STRATEGY_ID ORDER BY strategy_id
END

DROP TABLE #GROUP

RETURN 0
go
IF OBJECT_ID('dbo.access_strategy_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.access_strategy_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.access_strategy_get >>>'
go

DROP TABLE #STRATEGY_ID
