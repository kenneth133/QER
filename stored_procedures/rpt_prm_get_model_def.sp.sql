use QER
go
IF OBJECT_ID('dbo.rpt_prm_get_model_def') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rpt_prm_get_model_def
    IF OBJECT_ID('dbo.rpt_prm_get_model_def') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rpt_prm_get_model_def >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rpt_prm_get_model_def >>>'
END
go
CREATE PROCEDURE dbo.rpt_prm_get_model_def
AS

SELECT model_portfolio_def_cd
  FROM model_portfolio_def
 ORDER BY model_portfolio_def_cd

RETURN 0
go
IF OBJECT_ID('dbo.rpt_prm_get_model_def') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_prm_get_model_def >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_prm_get_model_def >>>'
go
