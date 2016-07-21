use QER
go

CREATE TABLE #DATA_SET (
  security_id	int		NULL,
  factor_value	float	NULL,
  ordinal		int identity(1,1) NOT NULL,
  rank			int		NULL
)

IF OBJECT_ID('dbo.rank_against_populate') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rank_against_populate
    IF OBJECT_ID('dbo.rank_against_populate') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rank_against_populate >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rank_against_populate >>>'
END
go
CREATE PROCEDURE dbo.rank_against_populate
@RANK_EVENT_ID int,
@DEBUG bit = NULL
AS

DECLARE @BDATE datetime,
        @UNIVERSE_DT datetime,
        @UNIVERSE_ID int,
        @AGAINST varchar(1),
        @AGAINST_CD varchar(8),
        @AGAINST_ID int,
        @SECTOR_MODEL_ID int

SELECT @BDATE = bdate,
       @UNIVERSE_DT = universe_dt,
       @UNIVERSE_ID = universe_id,
       @AGAINST = against,
       @AGAINST_CD = against_cd,
       @AGAINST_ID = against_id
  FROM rank_inputs
 WHERE rank_event_id = @RANK_EVENT_ID

IF @DEBUG = 1
BEGIN
  SELECT '@BDATE', @BDATE
  SELECT '@UNIVERSE_DT', @UNIVERSE_DT
  SELECT '@UNIVERSE_ID', @UNIVERSE_ID
  SELECT '@AGAINST', @AGAINST
  SELECT '@AGAINST_CD', @AGAINST_CD
  SELECT '@AGAINST_ID', @AGAINST_ID
END

IF @AGAINST IN ('C','G')
BEGIN
  IF @AGAINST = 'C'
  BEGIN
    SELECT @SECTOR_MODEL_ID = sector_model_id
      FROM sector_def
     WHERE sector_id = @AGAINST_ID
  END
  IF @AGAINST = 'G'
  BEGIN
    SELECT @SECTOR_MODEL_ID = c.sector_model_id
      FROM sector_def c, segment_def g
     WHERE g.segment_id = @AGAINST_ID
       AND g.sector_id = c.sector_id
  END

  IF EXISTS (SELECT 1 FROM universe_makeup p
              WHERE p.universe_dt = @UNIVERSE_DT
                AND p.universe_id = @UNIVERSE_ID
                AND NOT EXISTS (SELECT 1 FROM sector_model_security ss
                                 WHERE ss.bdate = @BDATE
                                   AND ss.sector_model_id = @SECTOR_MODEL_ID
                                   AND ss.security_id = p.security_id))
  BEGIN
    EXEC sector_model_security_populate @BDATE=@BDATE, @SECTOR_MODEL_ID=@SECTOR_MODEL_ID, @UNIVERSE_DT=@UNIVERSE_DT, @UNIVERSE_ID=@UNIVERSE_ID, @DEBUG=@DEBUG
  END

  IF @AGAINST = 'C'
  BEGIN
    INSERT #DATA_SET (security_id)
    SELECT ss.security_id
      FROM sector_model_security ss, universe_makeup p
     WHERE ss.bdate = @BDATE
       AND ss.sector_model_id = @SECTOR_MODEL_ID
       AND ss.sector_id = @AGAINST_ID
       AND p.universe_dt = @UNIVERSE_DT
       AND p.universe_id = @UNIVERSE_ID
       AND ss.security_id = p.security_id
  END
  IF @AGAINST = 'G'
  BEGIN
    INSERT #DATA_SET (security_id)
    SELECT ss.security_id
      FROM sector_model_security ss, universe_makeup p
     WHERE ss.bdate = @BDATE
       AND ss.sector_model_id = @SECTOR_MODEL_ID
       AND ss.segment_id = @AGAINST_ID
       AND p.universe_dt = @UNIVERSE_DT
       AND p.universe_id = @UNIVERSE_ID
       AND ss.security_id = p.security_id
  END
END
ELSE IF @AGAINST = 'R'
BEGIN
  INSERT #DATA_SET (security_id)
  SELECT p.security_id
    FROM universe_makeup p, region_makeup r, equity_common..security y
   WHERE p.universe_dt = @UNIVERSE_DT
     AND p.universe_id = @UNIVERSE_ID
     AND r.region_id = @AGAINST_ID
     AND p.security_id = y.security_id
     AND r.country_cd = ISNULL(y.domicile_iso_cd, y.issue_country_cd)
END
ELSE IF @AGAINST = 'Y'
BEGIN
  INSERT #DATA_SET (security_id)
  SELECT p.security_id
    FROM universe_makeup p, equity_common..security y
   WHERE p.universe_dt = @UNIVERSE_DT
     AND p.universe_id = @UNIVERSE_ID
     AND p.security_id = y.security_id
     AND ISNULL(y.domicile_iso_cd, y.issue_country_cd) = @AGAINST_CD
END
ELSE IF @AGAINST = 'U'
BEGIN
  INSERT #DATA_SET (security_id)
  SELECT security_id
    FROM universe_makeup
   WHERE universe_dt = @UNIVERSE_DT
     AND universe_id = @UNIVERSE_ID
END

IF @DEBUG = 1
BEGIN
  SELECT '#DATA_SET: rank_against_populate'
  SELECT * FROM #DATA_SET ORDER BY security_id
END

RETURN 0
go
IF OBJECT_ID('dbo.rank_against_populate') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rank_against_populate >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rank_against_populate >>>'
go

DROP TABLE #DATA_SET
go
