use QER
go
IF OBJECT_ID('dbo.position_sector_classify') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.position_sector_classify
    IF OBJECT_ID('dbo.position_sector_classify') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.position_sector_classify >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.position_sector_classify >>>'
END
go
CREATE PROCEDURE dbo.position_sector_classify @BDATE datetime = NULL
AS

DECLARE @BDATE2 datetime

IF @BDATE IS NULL
  BEGIN EXEC business_date_get @DIFF=-1, @RET_DATE=@BDATE2 OUTPUT END
ELSE
  BEGIN SELECT @BDATE2 = @BDATE END

CREATE TABLE #SECTOR_MODEL_ACCOUNT (
  sector_model_id	int				NOT NULL,
  account_cd		varchar (32)	NOT NULL
)

INSERT #SECTOR_MODEL_ACCOUNT
SELECT DISTINCT f.sector_model_id, a.account_cd
  FROM strategy g, factor_model f, account a
 WHERE a.strategy_id = g.strategy_id
   AND g.factor_model_id = f.factor_model_id

DECLARE @DUMMY_UNIVERSE_ID int,
        @SECTOR_MODEL_ID int
SELECT @DUMMY_UNIVERSE_ID = universe_id FROM universe_def WHERE universe_cd = 'DUMMY'

SELECT @SECTOR_MODEL_ID = 0
WHILE EXISTS (SELECT * FROM #SECTOR_MODEL_ACCOUNT WHERE sector_model_id > @SECTOR_MODEL_ID)
BEGIN
  SELECT @SECTOR_MODEL_ID = MIN(sector_model_id)
    FROM #SECTOR_MODEL_ACCOUNT
   WHERE sector_model_id > @SECTOR_MODEL_ID

  INSERT universe_makeup (universe_dt, universe_id, security_id)
  SELECT DISTINCT @BDATE2, @DUMMY_UNIVERSE_ID, p.security_id
    FROM equity_common..position p
   WHERE p.reference_date = @BDATE2
     AND p.reference_date = p.effective_date
     AND p.acct_cd IN (SELECT DISTINCT acct_cd
                         FROM equity_common..account a,
                             (SELECT DISTINCT account_cd FROM #SECTOR_MODEL_ACCOUNT WHERE sector_model_id = @SECTOR_MODEL_ID) x
                        WHERE a.parent = x.account_cd OR a.acct_cd = x.account_cd)
     AND NOT EXISTS (SELECT 1 FROM sector_model_security ss
                      WHERE ss.bdate = @BDATE2
                        AND ss.sector_model_id = @SECTOR_MODEL_ID
                        AND ss.security_id = p.security_id)

  IF EXISTS (SELECT * FROM universe_makeup WHERE universe_id = @DUMMY_UNIVERSE_ID)
  BEGIN
    EXEC sector_model_security_populate @BDATE=@BDATE2, @SECTOR_MODEL_ID=@SECTOR_MODEL_ID, @UNIVERSE_DT=@BDATE2, @UNIVERSE_ID=@DUMMY_UNIVERSE_ID
    DELETE universe_makeup WHERE universe_id = @DUMMY_UNIVERSE_ID
  END
END

DROP TABLE #SECTOR_MODEL_ACCOUNT

RETURN 0
go
IF OBJECT_ID('dbo.position_sector_classify') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.position_sector_classify >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.position_sector_classify >>>'
go
