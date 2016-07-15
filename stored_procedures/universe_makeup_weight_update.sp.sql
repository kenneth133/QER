use QER
go
IF OBJECT_ID('dbo.universe_makeup_weight_update') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.universe_makeup_weight_update
    IF OBJECT_ID('dbo.universe_makeup_weight_update') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.universe_makeup_weight_update >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.universe_makeup_weight_update >>>'
END
go
CREATE PROCEDURE dbo.universe_makeup_weight_update @UNIVERSE_DT datetime,
                                                   @UNIVERSE_CD varchar(32) = NULL,
                                                   @UNIVERSE_ID int = NULL,
                                                   @WEIGHT varchar(32) = 'CAP'
AS

IF @UNIVERSE_DT IS NULL
  BEGIN SELECT 'ERROR: @UNIVERSE_DT IS A REQUIRED PARAMETER' RETURN -1 END
IF @UNIVERSE_CD IS NULL AND @UNIVERSE_ID IS NULL
  BEGIN SELECT 'ERROR: EITHER @UNIVERSE_CD OR @UNIVERSE_ID PARAMETER MUST BE PASSED' RETURN -1 END
IF @WEIGHT NOT IN ('CAP')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @WEIGHT PARAMETER' RETURN -1 END

IF @UNIVERSE_ID IS NULL
BEGIN
  SELECT @UNIVERSE_ID = universe_id
    FROM universe_def
   WHERE universe_cd = @UNIVERSE_CD
END

CREATE TABLE #UNIV (
  ticker	varchar(16)		NULL,
  cusip		varchar(32)		NULL,
  sedol		varchar(32)		NULL,
  isin		varchar(64)		NULL,
  mkt_cap	float			NULL,
  weight	float			NULL
)

IF @WEIGHT = 'CAP'
BEGIN
  INSERT #UNIV
  SELECT i.ticker, i.cusip, i.sedol, i.isin, i.mkt_cap, NULL
    FROM universe_makeup p, instrument_characteristics i
   WHERE p.universe_dt = @UNIVERSE_DT
     AND p.universe_id = @UNIVERSE_ID
     AND i.bdate = p.universe_dt
     AND i.cusip = p.cusip

  UPDATE #UNIV
     SET mkt_cap = 0.0
   WHERE mkt_cap IS NULL

  UPDATE #UNIV
     SET weight = mkt_cap / x.[SUM_MCAP]
    FROM (SELECT SUM(mkt_cap) AS [SUM_MCAP] FROM #UNIV) x

  UPDATE universe_makeup
     SET weight = v.weight * 100.0
    FROM #UNIV v
   WHERE universe_makeup.universe_dt = @UNIVERSE_DT
     AND universe_makeup.universe_id = @UNIVERSE_ID
     AND universe_makeup.cusip = v.cusip
END

DROP TABLE #UNIV

RETURN 0
go
IF OBJECT_ID('dbo.universe_makeup_weight_update') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.universe_makeup_weight_update >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.universe_makeup_weight_update >>>'
go
