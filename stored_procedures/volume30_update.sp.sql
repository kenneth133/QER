use QER
go
IF OBJECT_ID('dbo.volume30_update') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.volume30_update
    IF OBJECT_ID('dbo.volume30_update') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.volume30_update >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.volume30_update >>>'
END
go
CREATE PROCEDURE dbo.volume30_update @BEGIN_DATE datetime = NULL,
                                     @END_DATE datetime = NULL
AS

CREATE TABLE #V30 (
  bdate		datetime	NOT NULL,
  mqa_id	varchar(32)	NULL,
  volume30	float		NULL
)

INSERT #V30
SELECT i1.bdate, i1.mqa_id, avg(i2.volume)
  FROM QER..instrument_characteristics i1,
       QER..instrument_characteristics i2
 WHERE i2.bdate >= dateadd(dd, -30, i1.bdate)
   AND i2.bdate <= i1.bdate
   AND i1.mqa_id = i2.mqa_id
   AND (@BEGIN_DATE IS NULL OR i1.bdate >= @BEGIN_DATE)
   AND (@END_DATE IS NULL OR i1.bdate <= @END_DATE)
 GROUP BY i1.bdate, i1.mqa_id

UPDATE QER..instrument_characteristics
   SET volume30 = #V30.volume30
  FROM #V30
 WHERE QER..instrument_characteristics.bdate = #V30.bdate
   AND QER..instrument_characteristics.mqa_id = #V30.mqa_id

DROP TABLE #V30

RETURN 0
go
IF OBJECT_ID('dbo.volume30_update') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.volume30_update >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.volume30_update >>>'
go