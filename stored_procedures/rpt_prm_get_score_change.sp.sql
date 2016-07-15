use QER
go
IF OBJECT_ID('dbo.rpt_prm_get_score_change') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rpt_prm_get_score_change
    IF OBJECT_ID('dbo.rpt_prm_get_score_change') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rpt_prm_get_score_change >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rpt_prm_get_score_change >>>'
END
go
CREATE PROCEDURE dbo.rpt_prm_get_score_change @STRATEGY_ID int
AS

SELECT fractile / 5
  FROM strategy
 WHERE strategy_id = @STRATEGY_ID

RETURN 0
go
IF OBJECT_ID('dbo.rpt_prm_get_score_change') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_prm_get_score_change >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_prm_get_score_change >>>'
go
