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

DECLARE @MKT_CAP_SUM float

CREATE TABLE #UNIV (
  security_id	int		NULL,
  mkt_cap		float	NULL,
  weight		float	NULL
)

IF @WEIGHT = 'CAP'
BEGIN
  INSERT #UNIV
  SELECT p.security_id, p.market_cap_usd, NULL
    FROM universe_makeup u, equity_common..market_price p
   WHERE u.universe_dt = @UNIVERSE_DT
     AND u.universe_id = @UNIVERSE_ID
     AND u.universe_dt = p.reference_date
     AND u.security_id = p.security_id

  UPDATE #UNIV
     SET mkt_cap = 0.0
   WHERE mkt_cap IS NULL

  SELECT @MKT_CAP_SUM = SUM(mkt_cap)
    FROM #UNIV

  IF @MKT_CAP_SUM = 0
    BEGIN UPDATE #UNIV SET weight = 0.0 END
  ELSE
    BEGIN UPDATE #UNIV SET weight = mkt_cap / @MKT_CAP_SUM END

  UPDATE universe_makeup
     SET weight = v.weight * 100.0
    FROM #UNIV v
   WHERE universe_makeup.universe_dt = @UNIVERSE_DT
     AND universe_makeup.universe_id = @UNIVERSE_ID
     AND universe_makeup.security_id = v.security_id
END

DROP TABLE #UNIV

RETURN 0
go
IF OBJECT_ID('dbo.universe_makeup_weight_update') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.universe_makeup_weight_update >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.universe_makeup_weight_update >>>'
go
