if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[rpt_prm_get_strategy]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[rpt_prm_get_strategy]
GO

CREATE PROCEDURE dbo.rpt_prm_get_strategy  
AS  
  
SELECT strategy_id, strategy_cd  
FROM strategy  
WHERE dbo.udf_check_user_access(strategy_id) = 'Y'
 ORDER BY strategy_cd  
  
RETURN 0