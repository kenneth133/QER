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
  BEGIN EXEC QER..business_date_get @DIFF=-1, @RET_DATE=@BDATE2 OUTPUT END
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

  INSERT universe_makeup (universe_dt, universe_id, mqa_id, ticker, cusip, sedol, isin, gv_key)
  SELECT DISTINCT @BDATE2, @DUMMY_UNIVERSE_ID, i.mqa_id, i.ticker, i.cusip, i.sedol, i.isin, i.gv_key
    FROM instrument_characteristics i,
        (SELECT DISTINCT p.cusip
           FROM #SECTOR_MODEL_ACCOUNT a, position p
          WHERE a.sector_model_id = @SECTOR_MODEL_ID
            AND a.account_cd = p.account_cd
            AND p.bdate = @BDATE2) x
   WHERE i.bdate = @BDATE2
     AND i.cusip = x.cusip
     AND NOT EXISTS (SELECT * FROM sector_model_security ss
                      WHERE ss.bdate = @BDATE2
                        AND ss.sector_model_id = @SECTOR_MODEL_ID
                        AND ss.cusip = i.cusip)

  IF EXISTS (SELECT * FROM universe_makeup WHERE universe_id = @DUMMY_UNIVERSE_ID)
  BEGIN
    EXEC sector_model_security_populate @BDATE2, @SECTOR_MODEL_ID, @BDATE2, @DUMMY_UNIVERSE_ID
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
