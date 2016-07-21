use QER
go

CREATE TABLE #DATA_SET (
  security_id	int		NULL,
  factor_value	float	NULL,
  ordinal		int identity(1,1) NOT NULL,
  rank			int		NULL
)

IF OBJECT_ID('dbo.rank_factor_populate') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rank_factor_populate
    IF OBJECT_ID('dbo.rank_factor_populate') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rank_factor_populate >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rank_factor_populate >>>'
END
go
CREATE PROCEDURE dbo.rank_factor_populate
@RANK_EVENT_ID int,
@DEBUG bit = NULL
AS

DECLARE @BDATE datetime,
        @AS_OF_DATE datetime,
        @FACTOR_ID int,
        @FACTOR_SOURCE_CD varchar(8),
        @RANK_WGT_ID int,
        @PERIOD_TYPE varchar(1),
        @MISSING_METHOD varchar(8),
        @MISSING_VALUE float

SELECT @BDATE = bdate,
       @AS_OF_DATE = as_of_date,
       @FACTOR_ID = factor_id,
       @FACTOR_SOURCE_CD = factor_source_cd,
       @RANK_WGT_ID = rank_wgt_id,
       @PERIOD_TYPE = period_type,
       @MISSING_METHOD = missing_method,
       @MISSING_VALUE = missing_value
  FROM rank_inputs
 WHERE rank_event_id = @RANK_EVENT_ID

/*
SMOOTH RANK SECTION HAS BEEN REMOVED BECAUSE NOT USED IN PRODUCTION.
FOR SOURCE CODE, SEE PREVIOUS VERSIONS OF THIS PROCEDURE IN SOURCE SAFE.
-KENNETH LEE, 5/19/2008
*/

IF @RANK_WGT_ID IS NULL --STRAIGHT RANK
BEGIN
  UPDATE #DATA_SET
     SET factor_value = f.factor_value
    FROM instrument_factor f,
         (SELECT i.security_id, MAX(i.update_tm) AS [update_tm]
            FROM instrument_factor i, #DATA_SET d
           WHERE i.bdate = @BDATE
             AND i.factor_id = @FACTOR_ID
             AND i.security_id = d.security_id
             AND i.source_cd = ISNULL(@FACTOR_SOURCE_CD,i.source_cd)
             AND update_tm <= @AS_OF_DATE
           GROUP BY i.security_id) x
   WHERE f.bdate = @BDATE
     AND f.security_id = #DATA_SET.security_id
     AND f.security_id = x.security_id
     AND f.factor_id = @FACTOR_ID
     AND f.source_cd = ISNULL(@FACTOR_SOURCE_CD,f.source_cd)
     AND f.update_tm = x.update_tm

  CREATE TABLE #NULL_SET (
    security_id		int		NULL,
    factor_value	float	NULL
  )

  INSERT #NULL_SET
  SELECT security_id, factor_value
    FROM #DATA_SET

  TRUNCATE TABLE #DATA_SET

  INSERT #DATA_SET (security_id, factor_value)
  SELECT security_id, factor_value
    FROM #NULL_SET
   WHERE factor_value IS NOT NULL
   ORDER BY factor_value, security_id

  DELETE #NULL_SET WHERE factor_value IS NOT NULL

  IF @DEBUG = 1
  BEGIN
    SELECT '#DATA_SET: rank_factor_populate (1)'
    SELECT * FROM #DATA_SET ORDER BY ordinal, security_id

    SELECT '#NULL_SET: rank_factor_populate (1)'
    SELECT * FROM #NULL_SET ORDER BY security_id
  END

  IF EXISTS (SELECT 1 FROM #NULL_SET)
  BEGIN
    IF @MISSING_VALUE IS NULL
    BEGIN
      IF @MISSING_METHOD = 'MODE'
      BEGIN
        SELECT TOP 1 @MISSING_VALUE = factor_value
          FROM #DATA_SET
         GROUP BY factor_value
         ORDER BY COUNT(*) DESC
      END
      ELSE IF @MISSING_METHOD = 'MIN'
        BEGIN SELECT @MISSING_VALUE = MIN(factor_value) FROM #DATA_SET END
      ELSE IF @MISSING_METHOD = 'MAX'
        BEGIN SELECT @MISSING_VALUE = MAX(factor_value) FROM #DATA_SET END
      ELSE IF @MISSING_METHOD = 'MEDIAN'
      BEGIN
        SELECT @MISSING_VALUE = factor_value
          FROM #DATA_SET
         WHERE ordinal = FLOOR((SELECT MAX(ordinal) FROM #DATA_SET)/2)
      END
    END

    UPDATE #NULL_SET SET factor_value = @MISSING_VALUE

    IF @DEBUG = 1
    BEGIN
      SELECT '#NULL_SET: rank_factor_populate (2)'
      SELECT * FROM #NULL_SET ORDER BY security_id
    END

    INSERT #NULL_SET
    SELECT security_id, factor_value
      FROM #DATA_SET

    TRUNCATE TABLE #DATA_SET

    INSERT #DATA_SET (security_id, factor_value)
    SELECT security_id, factor_value
      FROM #NULL_SET
     ORDER BY factor_value, security_id
  END

  DROP TABLE #NULL_SET
END

IF @DEBUG = 1
BEGIN
  SELECT '#DATA_SET: rank_factor_populate (2)'
  SELECT * FROM #DATA_SET ORDER BY ordinal, security_id
END

RETURN 0
go
IF OBJECT_ID('dbo.rank_factor_populate') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rank_factor_populate >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rank_factor_populate >>>'
go

DROP TABLE #DATA_SET
go
