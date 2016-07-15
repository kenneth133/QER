use QER
go
IF OBJECT_ID('dbo.rpt_prm_get_account') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rpt_prm_get_account
    IF OBJECT_ID('dbo.rpt_prm_get_account') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rpt_prm_get_account >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rpt_prm_get_account >>>'
END
go
CREATE PROCEDURE dbo.rpt_prm_get_account @STRATEGY_ID int
AS

IF @STRATEGY_ID IS NOT NULL
BEGIN
  SELECT account_cd
    FROM account
   WHERE strategy_id = @STRATEGY_ID
   ORDER BY representative DESC, account_cd
END
ELSE
BEGIN
  SELECT DISTINCT account_cd, representative
    FROM account
   ORDER BY representative DESC, account_cd
END

RETURN 0
go
IF OBJECT_ID('dbo.rpt_prm_get_account') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_prm_get_account >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_prm_get_account >>>'
go
