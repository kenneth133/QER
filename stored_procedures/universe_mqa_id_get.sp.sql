use QER
go

CREATE TABLE #UNIVERSE_IDS ( universe_id int NOT NULL )
CREATE TABLE #DATES (
  start_date	datetime	NOT NULL,
  end_date	datetime	NOT NULL
)

IF OBJECT_ID('dbo.universe_mqa_id_get') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.universe_mqa_id_get
    IF OBJECT_ID('dbo.universe_mqa_id_get') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.universe_mqa_id_get >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.universe_mqa_id_get >>>'
END
go
CREATE PROCEDURE dbo.universe_mqa_id_get @DATE_FORMAT int = NULL,
                                         @DEBUG bit = NULL
AS

CREATE TABLE #WANT (
  start_date	datetime	NOT NULL,
  end_date	datetime	NOT NULL,
  universe_id	int		NOT NULL
)

CREATE TABLE #HAVE (
  end_date	datetime	NULL,
  universe_id	int		NULL,
  universe_dt	datetime	NULL
)

INSERT #WANT
SELECT d.start_date, d.end_date, u.universe_id
  FROM #DATES d, #UNIVERSE_IDS u

IF @DEBUG = 1
BEGIN
  SELECT '#WANT: universe_mqa_id_get'
  SELECT * FROM #WANT
END

INSERT #HAVE
SELECT w.end_date, p.universe_id, p.universe_dt
  FROM QER..universe_makeup p, #WANT w
 WHERE p.universe_dt >= w.start_date
   AND p.universe_dt <= w.end_date
   AND p.universe_id = w.universe_id

DELETE #WANT
  FROM #HAVE h
 WHERE #WANT.end_date = h.end_date
   AND #WANT.universe_id = h.universe_id

IF @DEBUG = 1
BEGIN
  SELECT '#HAVE: universe_mqa_id_get'
  SELECT * FROM #HAVE

  SELECT '#WANT: universe_mqa_id_get'
  SELECT * FROM #WANT
END

INSERT #HAVE
SELECT w.end_date, p.universe_id, max(p.universe_dt)
  FROM QER..universe_makeup p, #WANT w
 WHERE p.universe_dt <= w.start_date
   AND p.universe_dt >= dateadd(mm, -1, start_date)
   AND p.universe_id = w.universe_id
 GROUP BY w.end_date, p.universe_id

INSERT #HAVE
SELECT w.end_date, p.universe_id, min(p.universe_dt)
  FROM QER..universe_makeup p, #WANT w
 WHERE p.universe_dt >= w.end_date
   AND p.universe_dt <= dateadd(dd, 31, end_date)
   AND p.universe_id = w.universe_id
 GROUP BY w.end_date, p.universe_id

IF @DEBUG = 1
BEGIN
  DELETE #WANT
    FROM #HAVE h
   WHERE #WANT.end_date = h.end_date
     AND #WANT.universe_id = h.universe_id

  SELECT '#HAVE: universe_mqa_id_get'
  SELECT * FROM #HAVE

  SELECT '#WANT: universe_mqa_id_get'
  SELECT * FROM #WANT
END

IF @DATE_FORMAT IS NOT NULL
BEGIN
  SELECT DISTINCT convert(varchar, h.end_date, @DATE_FORMAT), p.mqa_id
    FROM QER..universe_makeup p, #HAVE h
   WHERE p.universe_id = h.universe_id
     AND p.universe_dt = h.universe_dt
   ORDER BY convert(varchar, h.end_date, @DATE_FORMAT), p.mqa_id
END
ELSE
BEGIN
  SELECT DISTINCT h.end_date, p.mqa_id
    FROM QER..universe_makeup p, #HAVE h
   WHERE p.universe_id = h.universe_id
     AND p.universe_dt = h.universe_dt
   ORDER BY h.end_date, p.mqa_id
END

RETURN 0
go
IF OBJECT_ID('dbo.universe_mqa_id_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.universe_mqa_id_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.universe_mqa_id_get >>>'
go

DROP TABLE #DATES
DROP TABLE #UNIVERSE_IDS