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

IF OBJECT_ID('dbo.rank_factor_populate') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rank_factor_populate
    IF OBJECT_ID('dbo.rank_factor_populate') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rank_factor_populate >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rank_factor_populate >>>'
END
go
CREATE PROCEDURE dbo.rank_factor_populate @RANK_EVENT_ID int,
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
  FROM QER..rank_inputs
 WHERE rank_event_id = @RANK_EVENT_ID

IF @RANK_WGT_ID IS NOT NULL --SMOOTH RANK
BEGIN
  DECLARE @ADATE datetime,
          @TOTAL_WGT float

  CREATE TABLE #PRIOR_BDATES (
    period_back	int		NOT NULL,
    period_wgt	float		NULL,
    adate	datetime	NULL,
    bdate	datetime	NULL
  )

  IF @PERIOD_TYPE IN ('YY','YYYY')
  BEGIN
    INSERT #PRIOR_BDATES
    SELECT period_back, period_wgt, dateadd(yy, -period_back, @BDATE), NULL
      FROM QER..rank_weight
     WHERE rank_wgt_id = @RANK_WGT_ID
  END
  ELSE IF @PERIOD_TYPE IN ('QQ','Q')
  BEGIN
    INSERT #PRIOR_BDATES
    SELECT period_back, period_wgt, dateadd(qq, -period_back, @BDATE), NULL
      FROM QER..rank_weight
     WHERE rank_wgt_id = @RANK_WGT_ID
  END
  ELSE IF @PERIOD_TYPE IN ('MM','M')
  BEGIN
    INSERT #PRIOR_BDATES
    SELECT period_back, period_wgt, dateadd(mm, -period_back, @BDATE), NULL
      FROM QER..rank_weight
     WHERE rank_wgt_id = @RANK_WGT_ID
  END
  ELSE IF @PERIOD_TYPE IN ('WK','WW')
  BEGIN
    INSERT #PRIOR_BDATES
    SELECT period_back, period_wgt, dateadd(wk, -period_back, @BDATE), NULL
      FROM QER..rank_weight
     WHERE rank_wgt_id = @RANK_WGT_ID
  END
  ELSE IF @PERIOD_TYPE IN ('DD','D')
  BEGIN
    INSERT #PRIOR_BDATES
    SELECT period_back, period_wgt, dateadd(dd, -period_back, @BDATE), NULL
      FROM QER..rank_weight
     WHERE rank_wgt_id = @RANK_WGT_ID
  END

  UPDATE #PRIOR_BDATES
     SET bdate = adate
    FROM QER..instrument_factor i
   WHERE i.bdate = #PRIOR_BDATES.adate
     AND i.factor_id = @FACTOR_ID
     AND i.update_tm <= @AS_OF_DATE
     AND (@FACTOR_SOURCE_CD IS NULL OR i.source_cd = @FACTOR_SOURCE_CD)
     AND i.cusip IN (SELECT cusip FROM #DATA_SET)

  WHILE EXISTS (SELECT * FROM #PRIOR_BDATES WHERE bdate IS NULL)
  BEGIN
    SELECT @ADATE = min(adate)
      FROM #PRIOR_BDATES
     WHERE bdate IS NULL

    WHILE NOT EXISTS (SELECT * FROM QER..instrument_factor
                       WHERE bdate = @ADATE
                         AND factor_id = @FACTOR_ID
                         AND update_tm <= @AS_OF_DATE
                         AND (@FACTOR_SOURCE_CD IS NULL OR source_cd = @FACTOR_SOURCE_CD)
                         AND cusip IN (SELECT cusip FROM #DATA_SET))
    BEGIN
      SELECT @ADATE = dateadd(dd, -1, @ADATE)
    END

    UPDATE #PRIOR_BDATES
       SET bdate = @ADATE
     WHERE adate = (SELECT min(adate) FROM #PRIOR_BDATES WHERE bdate IS NULL)
  END

  CREATE TABLE #WGT_FACTOR (
    bdate		datetime	NOT NULL,
    cusip		varchar(32)	NULL,
    factor_value	float		NULL,
    wgt_factor_value	float		NULL
  )

  CREATE TABLE #NOT_NULL_SET (
    ordinal		int identity(1,1)	NOT NULL,
    factor_value	float			NULL
  )

  CREATE TABLE #DONE (
    bdate	datetime	NOT NULL
  )

  INSERT #WGT_FACTOR
  SELECT i.bdate, i.cusip, i.factor_value, NULL
    FROM QER..instrument_factor i, #PRIOR_BDATES p
   WHERE i.bdate = p.bdate
     AND i.factor_id = @FACTOR_ID
     AND i.update_tm <= @AS_OF_DATE
     AND (@FACTOR_SOURCE_CD IS NULL OR i.source_cd = @FACTOR_SOURCE_CD)
     AND i.cusip IN (SELECT cusip FROM #DATA_SET)

  WHILE EXISTS (SELECT * FROM #PRIOR_BDATES WHERE bdate NOT IN (SELECT bdate FROM #DONE))
  BEGIN
    SELECT @ADATE = min(bdate)
      FROM #PRIOR_BDATES

    IF EXISTS (SELECT * FROM #WGT_FACTOR WHERE bdate = @ADATE AND factor_value IS NULL)
    BEGIN
      IF @MISSING_VALUE IS NULL
      BEGIN
        TRUNCATE TABLE #NOT_NULL_SET

        INSERT #NOT_NULL_SET (factor_value)
        SELECT factor_value
          FROM #WGT_FACTOR
         WHERE bdate = @ADATE
           AND factor_value IS NOT NULL
         ORDER BY factor_value

        IF @MISSING_METHOD = 'MODE'
          SELECT TOP 1 @MISSING_VALUE = factor_value
            FROM #NOT_NULL_SET
           ORDER BY count(*) DESC
        ELSE IF @MISSING_METHOD = 'MIN'
          SELECT @MISSING_VALUE = min(factor_value)
            FROM #NOT_NULL_SET
        ELSE IF @MISSING_METHOD = 'MAX'
          SELECT @MISSING_VALUE = max(factor_value)
            FROM #NOT_NULL_SET
        ELSE --MEDIAN
          SELECT @MISSING_VALUE = factor_value
            FROM #NOT_NULL_SET
           WHERE ordinal = floor((SELECT max(ordinal) FROM #NOT_NULL_SET)/2)
      END

      UPDATE #WGT_FACTOR
         SET factor_value = @MISSING_VALUE
       WHERE bdate = @ADATE
         AND factor_value IS NULL
    END

    INSERT #DONE
    SELECT @ADATE
  END

  SELECT @TOTAL_WGT = sum(period_wgt)
    FROM #PRIOR_BDATES

  UPDATE #WGT_FACTOR
     SET wgt_factor_value = factor_value * (p.period_wgt/@TOTAL_WGT)
    FROM #PRIOR_BDATES p
   WHERE p.bdate = #WGT_FACTOR.bdate

  UPDATE #DATA_SET
     SET factor_value = x.wgt_factor_value
    FROM (SELECT cusip, sum(wgt_factor_value) AS wgt_factor_value
            FROM #WGT_FACTOR
           GROUP BY cusip) x
   WHERE x.cusip = #DATA_SET.cusip

  DROP TABLE #DONE
  DROP TABLE #NOT_NULL_SET
  DROP TABLE #WGT_FACTOR
  DROP TABLE #PRIOR_BDATES
END
ELSE --STRAIGHT RANK
BEGIN
  WHILE NOT EXISTS (SELECT * FROM QER..instrument_factor
                     WHERE bdate = @BDATE
                       AND factor_id = @FACTOR_ID
                       AND update_tm <= @AS_OF_DATE
                       AND (@FACTOR_SOURCE_CD IS NULL OR source_cd = @FACTOR_SOURCE_CD)
                       AND cusip IN (SELECT cusip FROM #DATA_SET))
  BEGIN
    SELECT @BDATE = dateadd(dd, -1, @BDATE)
  END

  UPDATE #DATA_SET
     SET factor_value = f.factor_value
    FROM QER..instrument_factor f,
         (SELECT cusip, max(update_tm) as update_tm
            FROM QER..instrument_factor
           WHERE bdate = @BDATE
             AND cusip IN (SELECT cusip FROM #DATA_SET)
             AND factor_id = @FACTOR_ID
             AND (@FACTOR_SOURCE_CD IS NULL OR source_cd = @FACTOR_SOURCE_CD)
             AND update_tm <= @AS_OF_DATE
           GROUP BY cusip) x
   WHERE f.bdate = @BDATE
     AND f.cusip = #DATA_SET.cusip
     AND f.cusip = x.cusip
     AND f.factor_id = @FACTOR_ID
     AND (@FACTOR_SOURCE_CD IS NULL OR f.source_cd = @FACTOR_SOURCE_CD)
     AND f.update_tm = x.update_tm

  CREATE TABLE #NULL_SET (
    mqa_id		varchar(32)		NULL,
    ticker		varchar(16)		NULL,
    cusip		varchar(32)		NULL,
    sedol		varchar(32)		NULL,
    isin		varchar(64)		NULL,
    gv_key		int			NULL,
    factor_value	float			NULL
  )

  INSERT #NULL_SET
  SELECT mqa_id, ticker, cusip, sedol, isin, gv_key, factor_value
    FROM #DATA_SET

  TRUNCATE TABLE #DATA_SET

  INSERT #DATA_SET (mqa_id, ticker, cusip, sedol, isin, gv_key, factor_value)
  SELECT mqa_id, ticker, cusip, sedol, isin, gv_key, factor_value
    FROM #NULL_SET
   WHERE factor_value IS NOT NULL
   ORDER BY factor_value, cusip

  DELETE #NULL_SET
   WHERE factor_value IS NOT NULL

  IF @DEBUG = 1
  BEGIN
    SELECT '#DATA_SET: rank_factor_populate'
    SELECT * FROM #DATA_SET

    SELECT '#NULL_SET: rank_factor_populate'
    SELECT * FROM #NULL_SET
  END

  IF EXISTS (SELECT * FROM #NULL_SET)
  BEGIN
    IF @MISSING_VALUE IS NULL
    BEGIN
      IF @MISSING_METHOD = 'MODE'
        SELECT TOP 1 @MISSING_VALUE = factor_value
          FROM #DATA_SET
         GROUP BY factor_value
         ORDER BY count(*) DESC
      ELSE IF @MISSING_METHOD = 'MIN'
        SELECT @MISSING_VALUE = min(factor_value)
          FROM #DATA_SET
      ELSE IF @MISSING_METHOD = 'MAX'
        SELECT @MISSING_VALUE = max(factor_value)
          FROM #DATA_SET
      ELSE --MEDIAN
        SELECT @MISSING_VALUE = factor_value
          FROM #DATA_SET
         WHERE ordinal = floor((SELECT max(ordinal) FROM #DATA_SET)/2)
    END

    UPDATE #NULL_SET
       SET factor_value = @MISSING_VALUE

    IF @DEBUG = 1
    BEGIN
      SELECT '#NULL_SET: rank_factor_populate'
      SELECT * FROM #NULL_SET
    END

    INSERT #NULL_SET
    SELECT mqa_id, ticker, cusip, sedol, isin, gv_key, factor_value
      FROM #DATA_SET

    TRUNCATE TABLE #DATA_SET

    INSERT #DATA_SET (mqa_id, ticker, cusip, sedol, isin, gv_key, factor_value)
    SELECT mqa_id, ticker, cusip, sedol, isin, gv_key, factor_value
      FROM #NULL_SET
     ORDER BY factor_value, cusip
  END

  DROP TABLE #NULL_SET
END

IF @DEBUG = 1
BEGIN
  SELECT '#DATA_SET: rank_factor_populate'
  SELECT *
    FROM #DATA_SET
END

RETURN 0
go
IF OBJECT_ID('dbo.rank_factor_populate') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rank_factor_populate >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rank_factor_populate >>>'
go

DROP TABLE #DATA_SET