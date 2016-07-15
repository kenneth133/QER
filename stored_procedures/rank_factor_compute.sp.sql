use QER
go

CREATE TABLE #DATA_SET (
  mqa_id	varchar(32)		NULL,
  ticker	varchar(16)		NULL,
  cusip		varchar(32)		NULL,
  sedol		varchar(32)		NULL,
  isin		varchar(64)		NULL,
  gv_key	int			NULL,
  mkt_cap	float			NULL,
  factor_value	float			NULL,
  ordinal	int identity(1,1)	NOT NULL,
  rank		int			NULL,
  eq_return	float			NULL,
  cap_return	float			NULL
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
CREATE PROCEDURE dbo.rank_factor_compute @RANK_EVENT_ID int,
                                         @DEBUG bit = NULL
AS

DECLARE @GROUPS int,
        @METHOD varchar(4),
        @MAX_ORDINAL_P1 int

SELECT @GROUPS = groups,
       @METHOD = method
  FROM QER..rank_inputs
 WHERE rank_event_id = @RANK_EVENT_ID

CREATE TABLE #DISTINCT_SET (
  factor_value	float	NULL,
  mean		float	NULL,
  hi		float	NULL,
  lo		float	NULL
)

INSERT #DISTINCT_SET
SELECT factor_value, avg(convert(float,ordinal)), max(ordinal), min(ordinal)
  FROM #DATA_SET
 GROUP BY factor_value

IF @DEBUG = 1
BEGIN
  SELECT '#DISTINCT_SET: rank_factor_compute'
  SELECT * FROM #DISTINCT_SET
END

SELECT @MAX_ORDINAL_P1 = max(ordinal) + 1
  FROM #DATA_SET

UPDATE #DISTINCT_SET
   SET mean = floor(mean * @GROUPS / @MAX_ORDINAL_P1) + 1,
       hi = floor(hi * @GROUPS / @MAX_ORDINAL_P1) + 1,
       lo = floor(lo * @GROUPS / @MAX_ORDINAL_P1) + 1

UPDATE #DATA_SET
   SET rank = CASE WHEN @METHOD = 'MEAN' THEN d.mean
                   WHEN @METHOD LIKE 'HI%' THEN d.hi
                   WHEN @METHOD LIKE 'LO%' THEN d.lo
              END
  FROM #DISTINCT_SET d
 WHERE #DATA_SET.factor_value = d.factor_value

IF @DEBUG = 1
BEGIN
  SELECT '@MAX_ORDINAL_P1', @MAX_ORDINAL_P1

  SELECT '#DATA_SET: rank_factor_compute'
  SELECT * FROM #DATA_SET

  SELECT '#DISTINCT_SET: rank_factor_compute'
  SELECT * FROM #DISTINCT_SET
END

RETURN 0
go
IF OBJECT_ID('dbo.rank_factor_compute') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rank_factor_compute >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rank_factor_compute >>>'
go

DROP TABLE #DATA_SET