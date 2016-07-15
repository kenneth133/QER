use QER
go

CREATE TABLE #ENTIRE_SET (
  mqa_id	varchar(32)		NULL,
  cusip8	varchar(32)		NULL,
  ticker	varchar(16)		NULL,
  gv_key	int			NULL,
  mktcap	float			NULL,
  factor_value	float			NULL,
  ordinal	int identity(1,1)	NOT NULL,
  rank		int			NULL,
  eq_return	float			NULL,
  cap_return	float			NULL
)

IF OBJECT_ID('dbo.factor_returns_populate') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.factor_returns_populate
    IF OBJECT_ID('dbo.factor_returns_populate') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.factor_returns_populate >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.factor_returns_populate >>>'
END
go
CREATE PROCEDURE dbo.factor_returns_populate @BDATE datetime,
                                             @AS_OF_DATE datetime,
                                             @RETURN_FACTOR_ID int,
                                             @RETURN_FACTOR_SOURCE_CD varchar(8),
                                             @DEBUG bit = NULL
AS

DECLARE @RDATE datetime,
        @RETURN_FACTOR_CD varchar(64)

SELECT @RETURN_FACTOR_CD = factor_cd
  FROM QER..factor
 WHERE factor_id = @RETURN_FACTOR_ID

SELECT @RDATE = @BDATE

WHILE NOT EXISTS (SELECT * FROM QER..instrument_factor
                   WHERE bdate = @RDATE
                     AND factor_id = @RETURN_FACTOR_ID
                     AND update_tm <= @AS_OF_DATE
                     AND (@RETURN_FACTOR_SOURCE_CD IS NULL OR source_cd = @RETURN_FACTOR_SOURCE_CD)
                     AND cusip IN (SELECT cusip8 FROM #ENTIRE_SET))
BEGIN
  IF @RETURN_FACTOR_CD LIKE '%_MTH_END'
    SELECT @RDATE = dateadd(dd, 1, @RDATE)
  ELSE
    SELECT @RDATE = dateadd(dd, -1, @RDATE)
END

UPDATE #ENTIRE_SET
   SET eq_return = f.factor_value
  FROM QER..instrument_factor f,
       (SELECT cusip, max(update_tm) as update_tm
          FROM QER..instrument_factor
         WHERE bdate = @RDATE
           AND cusip IN (SELECT cusip8 FROM #ENTIRE_SET)
           AND factor_id = @RETURN_FACTOR_ID
           AND (@RETURN_FACTOR_SOURCE_CD IS NULL OR source_cd = @RETURN_FACTOR_SOURCE_CD)
           AND update_tm <= @AS_OF_DATE
         GROUP BY cusip) x
 WHERE f.bdate = @RDATE
   AND f.cusip = #ENTIRE_SET.cusip8
   AND f.cusip = x.cusip
   AND f.factor_id = @RETURN_FACTOR_ID
   AND (@RETURN_FACTOR_SOURCE_CD IS NULL OR f.source_cd = @RETURN_FACTOR_SOURCE_CD)
   AND f.update_tm = x.update_tm

WHILE NOT EXISTS (SELECT * FROM QER..instrument_characteristics
                   WHERE bdate = @BDATE
                     AND update_tm <= @AS_OF_DATE
                     AND cusip IN (SELECT cusip8 FROM #ENTIRE_SET))
BEGIN
  SELECT @BDATE = dateadd(dd, -1, @BDATE)
END

UPDATE #ENTIRE_SET
   SET mktcap = f.mktcap
  FROM QER..instrument_characteristics f,
       (SELECT cusip, max(update_tm) as update_tm
          FROM QER..instrument_characteristics
         WHERE bdate = @BDATE
           AND cusip IN (SELECT cusip8 FROM #ENTIRE_SET)
           AND (@RETURN_FACTOR_SOURCE_CD IS NULL OR source_cd = @RETURN_FACTOR_SOURCE_CD)
           AND update_tm <= @AS_OF_DATE
         GROUP BY cusip) x
 WHERE f.bdate = @BDATE
   AND f.cusip = #ENTIRE_SET.cusip8
   AND f.cusip = x.cusip
   AND (@RETURN_FACTOR_SOURCE_CD IS NULL OR f.source_cd = @RETURN_FACTOR_SOURCE_CD)
   AND f.update_tm = x.update_tm

DELETE #ENTIRE_SET
 WHERE eq_return IS NULL

UPDATE #ENTIRE_SET
   SET mktcap = 0
 WHERE mktcap IS NULL

CREATE TABLE #WGT_DENOMINATOR (
  rank		int	NOT NULL,
  sum_mktcap	float	NOT NULL
)

INSERT #WGT_DENOMINATOR
SELECT rank, sum(mktcap)
  FROM #ENTIRE_SET
 GROUP BY rank

IF @DEBUG = 1
BEGIN
  SELECT '#WGT_DENOMINATOR: factor_returns_populate'
  SELECT * FROM #WGT_DENOMINATOR
END

UPDATE #ENTIRE_SET
   SET cap_return = eq_return * (mktcap/d.sum_mktcap)
  FROM #WGT_DENOMINATOR d
 WHERE #ENTIRE_SET.rank = d.rank
   AND d.sum_mktcap != 0

UPDATE #ENTIRE_SET
   SET cap_return = 0
 WHERE cap_return IS NULL

IF @DEBUG = 1
BEGIN
  SELECT '#ENTIRE_SET: factor_returns_populate'
  SELECT * FROM #ENTIRE_SET
END

RETURN 0
go
IF OBJECT_ID('dbo.factor_returns_populate') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.factor_returns_populate >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.factor_returns_populate >>>'
go

DROP TABLE #ENTIRE_SET