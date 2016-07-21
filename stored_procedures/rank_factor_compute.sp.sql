use QER
go

CREATE TABLE #DATA_SET (
  security_id	int		NULL,
  factor_value	float	NULL,
  ordinal		int identity(1,1) NOT NULL,
  rank			int		NULL
)

IF OBJECT_ID('dbo.rank_factor_compute') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rank_factor_compute
    IF OBJECT_ID('dbo.rank_factor_compute') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rank_factor_compute >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rank_factor_compute >>>'
END
go
CREATE PROCEDURE dbo.rank_factor_compute
@RANK_EVENT_ID int,
@DEBUG bit = NULL
AS

DECLARE @GROUPS int,
        @METHOD varchar(4),
        @MAX_ORDINAL_P1 int

SELECT @GROUPS = groups,
       @METHOD = method
  FROM rank_inputs
 WHERE rank_event_id = @RANK_EVENT_ID

CREATE TABLE #DISTINCT_SET (
  factor_value	float	NULL,
  mean		float	NULL,
  hi		float	NULL,
  lo		float	NULL
)

INSERT #DISTINCT_SET
SELECT factor_value, AVG(CONVERT(float,ordinal)), MAX(ordinal), MIN(ordinal)
  FROM #DATA_SET
 GROUP BY factor_value

IF @DEBUG = 1
BEGIN
  SELECT '#DISTINCT_SET: rank_factor_compute (1)'
  SELECT * FROM #DISTINCT_SET ORDER BY factor_value
END

SELECT @MAX_ORDINAL_P1 = MAX(ordinal) + 1
  FROM #DATA_SET

IF @DEBUG = 1
  BEGIN SELECT '@MAX_ORDINAL_P1', @MAX_ORDINAL_P1 END

UPDATE #DISTINCT_SET
   SET mean = FLOOR(mean * CONVERT(float,@GROUPS) / CONVERT(float,@MAX_ORDINAL_P1)) + 1.0,
       hi = FLOOR(hi * CONVERT(float,@GROUPS) / CONVERT(float,@MAX_ORDINAL_P1)) + 1.0,
       lo = FLOOR(lo * CONVERT(float,@GROUPS) / CONVERT(float,@MAX_ORDINAL_P1)) + 1.0

IF @DEBUG = 1
BEGIN
  SELECT '#DISTINCT_SET: rank_factor_compute (2)'
  SELECT * FROM #DISTINCT_SET ORDER BY factor_value
END

UPDATE #DATA_SET
   SET rank = CASE WHEN @METHOD = 'MEAN' THEN CONVERT(int,ROUND(ROUND(d.mean,1),0))
                   WHEN @METHOD LIKE 'HI%' THEN CONVERT(int,ROUND(ROUND(d.hi,1),0))
                   WHEN @METHOD LIKE 'LO%' THEN CONVERT(int,ROUND(ROUND(d.lo,1),0)) END
  FROM #DISTINCT_SET d
 WHERE ISNULL(#DATA_SET.factor_value,ROUND(-999999999.0000,4)) = ISNULL(d.factor_value,ROUND(-999999999.0000,4))

IF @DEBUG = 1
BEGIN
  SELECT '#DATA_SET: rank_factor_compute'
  SELECT * FROM #DATA_SET ORDER BY ordinal, security_id
END

RETURN 0
go
IF OBJECT_ID('dbo.rank_factor_compute') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rank_factor_compute >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rank_factor_compute >>>'
go

DROP TABLE #DATA_SET
go
