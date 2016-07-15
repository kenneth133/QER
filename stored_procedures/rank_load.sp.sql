use QER
go
IF OBJECT_ID('dbo.rank_load') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rank_load
    IF OBJECT_ID('dbo.rank_load') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rank_load >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rank_load >>>'
END
go
CREATE PROCEDURE dbo.rank_load @RUN_TM datetime = NULL,
                               @AS_OF_DATE datetime = NULL,
                               @UNIVERSE_CD varchar(32),
                               @FACTOR_CD varchar(32),
                               @FACTOR_SOURCE_CD varchar(8) = NULL,
                               @GROUPS int,
                               @AGAINST varchar(1),
                               @AGAINST_ID int = NULL,
                               @RANK_WGT_ID int = NULL,
                               @PERIOD_TYPE varchar(1) = NULL,
                               @METHOD varchar(4) = 'MEAN',
                               @MISSING_METHOD varchar(8) = 'MEDIAN',
                               @MISSING_VALUE float = NULL
AS

IF NOT EXISTS (SELECT * FROM universe_def WHERE universe_cd = @UNIVERSE_CD)
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @UNIVERSE_CD PARAMETER' RETURN -1 END
IF NOT EXISTS (SELECT * FROM factor WHERE factor_cd = @FACTOR_CD)
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @FACTOR_CD PARAMETER' RETURN -1 END
IF @FACTOR_SOURCE_CD IS NOT NULL AND NOT EXISTS (SELECT * FROM decode WHERE item = 'SOURCE_CD' AND code = @FACTOR_SOURCE_CD)
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @FACTOR_SOURCE_CD PARAMETER' RETURN -1 END
IF @AGAINST NOT IN ('U', 'C', 'G')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @AGAINST PARAMETER' RETURN -1 END
IF @AGAINST != 'U' AND @AGAINST_ID IS NULL
  BEGIN SELECT 'ERROR: PARAMETER @AGAINST_ID CANNOT BE NULL' RETURN -1 END
IF @AGAINST = 'C' AND NOT EXISTS (SELECT * FROM sector_def WHERE sector_id = @AGAINST_ID)
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @AGAINST_ID PARAMETER' RETURN -1 END
IF @AGAINST = 'G' AND NOT EXISTS (SELECT * FROM segment_def WHERE segment_id = @AGAINST_ID)
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @AGAINST_ID PARAMETER' RETURN -1 END
IF @PERIOD_TYPE NOT IN ('YY', 'YYYY', 'QQ', 'Q', 'MM', 'M', 'WK', 'WW', 'DD', 'D')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @PERIOD_TYPE PARAMETER' RETURN -1 END
IF @METHOD != 'MEAN' AND @METHOD NOT LIKE 'HI%' AND @METHOD NOT LIKE 'LO%'
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @METHOD PARAMETER' RETURN -1 END
IF @MISSING_METHOD NOT IN ('MODE','MEDIAN','MIN','MAX')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @MISSING_METHOD PARAMETER' RETURN -1 END
IF (SELECT COUNT(DISTINCT bdate) FROM rank_staging) > 1
  BEGIN SELECT 'ERROR: CANNOT PROCESS MORE THAN ONE bdate FROM rank_staging AT A TIME' RETURN -1 END
IF (SELECT COUNT(DISTINCT universe_dt) FROM rank_staging) > 1
  BEGIN SELECT 'ERROR: CANNOT PROCESS MORE THAN ONE universe_dt FROM rank_staging AT A TIME' RETURN -1 END

IF @RUN_TM IS NULL
BEGIN
  IF @AS_OF_DATE IS NOT NULL
    BEGIN SELECT @RUN_TM = @AS_OF_DATE END
  ELSE
    BEGIN SELECT @RUN_TM = GETDATE() END
END

IF @AS_OF_DATE IS NULL
BEGIN
  IF @RUN_TM IS NOT NULL
    BEGIN SELECT @AS_OF_DATE = @RUN_TM END
  ELSE
    BEGIN SELECT @AS_OF_DATE = GETDATE() END
END

DECLARE @SECTOR_MODEL_ID int,
        @UNIVERSE_ID int,
        @UNIVERSE_DT datetime,
        @BDATE datetime,
        @DATE_ID int,
        @RANK_EVENT_ID int,
        @FACTOR_ID int

SELECT @UNIVERSE_ID = universe_id
  FROM universe_def
 WHERE universe_cd = @UNIVERSE_CD

SELECT @FACTOR_ID = factor_id
  FROM factor
 WHERE factor_cd = @FACTOR_CD

IF @AGAINST != 'U'
BEGIN
  IF @AGAINST = 'C'
  BEGIN
    SELECT @SECTOR_MODEL_ID = sector_model_id
      FROM sector_def
     WHERE sector_id = @AGAINST_ID
  END
  ELSE IF @AGAINST = 'G'
  BEGIN
    SELECT @SECTOR_MODEL_ID = c.sector_model_id
      FROM sector_def c, segment_def g
     WHERE c.sector_id = g.sector_id
       AND g.segment_id = @AGAINST_ID
  END

  SELECT @BDATE = bdate,
         @UNIVERSE_DT = universe_dt
    FROM rank_staging

  IF EXISTS (SELECT * FROM rank_staging
              WHERE cusip IS NOT NULL
                AND cusip NOT IN (SELECT cusip FROM sector_model_security
                                   WHERE bdate = @BDATE
                                     AND sector_model_id = @SECTOR_MODEL_ID
                                     AND cusip IS NOT NULL))
    BEGIN EXEC sector_model_security_populate @BDATE=@BDATE, @UNIVERSE_DT=@UNIVERSE_DT, @UNIVERSE_ID=@UNIVERSE_ID, @SECTOR_MODEL_ID=@SECTOR_MODEL_ID END
END

INSERT rank_inputs
      (run_tm, as_of_date, bdate, universe_dt, universe_id, factor_id, factor_source_cd,
       groups, against, against_id, rank_wgt_id, period_type, method, missing_method, missing_value)
SELECT DISTINCT @RUN_TM, @AS_OF_DATE, bdate, universe_dt, @UNIVERSE_ID, @FACTOR_ID, @FACTOR_SOURCE_CD,
       @GROUPS, @AGAINST, @AGAINST_ID, @RANK_WGT_ID, @PERIOD_TYPE, @METHOD, @MISSING_METHOD, @MISSING_VALUE
  FROM rank_staging

SELECT @RANK_EVENT_ID = MAX(rank_event_id)
  FROM rank_inputs

IF @AGAINST = 'U'
BEGIN
  INSERT rank_output
        (rank_event_id, mqa_id, ticker, cusip, sedol, isin, gv_key, factor_value, rank)
  SELECT @RANK_EVENT_ID, mqa_id, ticker, cusip, sedol, isin, gv_key, factor_value, rank
    FROM rank_staging
END
ELSE IF @AGAINST = 'C'
BEGIN
  INSERT rank_output
        (rank_event_id, mqa_id, ticker, cusip, sedol, isin, gv_key, factor_value, rank)
  SELECT @RANK_EVENT_ID, r.mqa_id, r.ticker, r.cusip, r.sedol, r.isin, r.gv_key, r.factor_value, r.rank
    FROM rank_staging r, sector_model_security ss
   WHERE ss.sector_model_id = @SECTOR_MODEL_ID
     AND r.bdate = ss.bdate
     AND r.cusip = ss.cusip
     AND ss.sector_id = @AGAINST_ID
END
ELSE IF @AGAINST = 'G'
BEGIN
  INSERT rank_output
        (rank_event_id, mqa_id, ticker, cusip, sedol, isin, gv_key, factor_value, rank)
  SELECT @RANK_EVENT_ID, r.mqa_id, r.ticker, r.cusip, r.sedol, r.isin, r.gv_key, r.factor_value, r.rank
    FROM rank_staging r, sector_model_security ss
   WHERE ss.sector_model_id = @SECTOR_MODEL_ID
     AND r.bdate = ss.bdate
     AND r.cusip = ss.cusip
     AND ss.segment_id = @AGAINST_ID
END

RETURN 0
go
IF OBJECT_ID('dbo.rank_load') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rank_load >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rank_load >>>'
go
