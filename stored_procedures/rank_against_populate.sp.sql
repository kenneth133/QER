use QER
go

CREATE TABLE #DATA_SET (
  security_id int		NULL,
  mkt_cap	float		NULL,
  factor_value float	NULL,
  ordinal	int identity(1,1) NOT NULL,
  rank		int			NULL,
  eq_return	float		NULL,
  cap_return float		NULL
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
CREATE PROCEDURE dbo.rank_against_populate @RANK_EVENT_ID int,
                                           @DEBUG bit = NULL
AS

DECLARE @BDATE datetime,
        @UNIVERSE_DT datetime,
        @UNIVERSE_ID int,
        @AGAINST varchar(1),
        @AGAINST_ID int,
        @SECTOR_MODEL_ID int

SELECT @BDATE = bdate,
       @UNIVERSE_ID = universe_id,
       @AGAINST = against,
       @AGAINST_ID = against_id
  FROM rank_inputs
 WHERE rank_event_id = @RANK_EVENT_ID

SELECT @UNIVERSE_DT = @BDATE
EXEC universe_date_get @UNIVERSE_ID, @UNIVERSE_DT OUTPUT --GIVEN @BDATE, ASSIGNS CLOSEST universe_dt to @UNIVERSE_DT

IF @DEBUG = 1
BEGIN
  SELECT '@BDATE', @BDATE
  SELECT '@UNIVERSE_DT', @UNIVERSE_DT
  SELECT '@UNIVERSE_ID', @UNIVERSE_ID
  SELECT '@AGAINST', @AGAINST
  SELECT '@AGAINST_ID', @AGAINST_ID
END

UPDATE rank_inputs
   SET universe_dt = @UNIVERSE_DT
 WHERE rank_event_id = @RANK_EVENT_ID

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

  IF EXISTS (SELECT * FROM universe_makeup
              WHERE universe_dt = @UNIVERSE_DT
                AND universe_id = @UNIVERSE_ID
                AND security_id IS NOT NULL
                AND security_id NOT IN (SELECT security_id FROM sector_model_security
                                         WHERE bdate = @BDATE
                                           AND sector_model_id = @SECTOR_MODEL_ID
                                           AND security_id IS NOT NULL))
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
IF @AGAINST = 'U'
BEGIN
  INSERT #DATA_SET (security_id)
  SELECT security_id
    FROM universe_makeup
   WHERE universe_id = @UNIVERSE_ID
     AND universe_dt = @UNIVERSE_DT
END

IF @DEBUG = 1
BEGIN
  SELECT '#DATA_SET: rank_against_populate'
  SELECT * FROM #DATA_SET
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
