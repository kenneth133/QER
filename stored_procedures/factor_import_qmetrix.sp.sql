use QER
go
IF OBJECT_ID('dbo.factor_import_qmetrix') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.factor_import_qmetrix
    IF OBJECT_ID('dbo.factor_import_qmetrix') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.factor_import_qmetrix >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.factor_import_qmetrix >>>'
END
go
CREATE PROCEDURE dbo.factor_import_qmetrix @BEGIN_DATE datetime = NULL,
                                           @END_DATE datetime = NULL,
                                           @DEBUG bit = NULL
AS

IF @BEGIN_DATE IS NULL OR @END_DATE IS NULL
BEGIN
  SELECT 'ERROR: @BEGIN_DATE AND @END_DATE ARE REQUIRED PARAMETERS'
  RETURN -1
END

CREATE TABLE #UNIVERSE_DATES (
  begin_dt	datetime	NULL,
  end_dt	datetime	NULL,
  universe_dt	datetime	NOT NULL,
  universe_id	int		NOT NULL
)

INSERT #UNIVERSE_DATES
SELECT DISTINCT NULL, NULL, universe_dt, universe_id
  FROM QER..universe_makeup
 WHERE universe_dt >= @BEGIN_DATE
   AND universe_dt <= @END_DATE

INSERT #UNIVERSE_DATES
SELECT NULL, NULL, min(universe_dt), universe_id
  FROM QER..universe_makeup
 WHERE universe_dt > @END_DATE
   AND universe_dt <= dateadd(dd, 31, @END_DATE)
   AND universe_id NOT IN (SELECT universe_id FROM #UNIVERSE_DATES WHERE universe_dt = @END_DATE)
 GROUP BY universe_id

UPDATE #UNIVERSE_DATES
   SET end_dt = universe_dt

UPDATE #UNIVERSE_DATES
   SET begin_dt = dateadd(dd, 1, x.prev_universe_dt)
  FROM (SELECT max(u1.end_dt) AS prev_universe_dt, u2.universe_dt, u1.universe_id
          FROM #UNIVERSE_DATES u1, #UNIVERSE_DATES u2
         WHERE u1.universe_id = u2.universe_id
           AND u1.end_dt < u2.end_dt
         GROUP BY u2.universe_dt, u1.universe_id) x
 WHERE #UNIVERSE_DATES.universe_id = x.universe_id
   AND #UNIVERSE_DATES.universe_dt = x.universe_dt

UPDATE #UNIVERSE_DATES
   SET begin_dt = @BEGIN_DATE
  FROM (SELECT min(universe_dt) AS min_universe_dt, universe_id
          FROM #UNIVERSE_DATES
         GROUP BY universe_id) x
 WHERE #UNIVERSE_DATES.universe_dt = x.min_universe_dt
   AND #UNIVERSE_DATES.universe_id = x.universe_id

UPDATE #UNIVERSE_DATES
   SET end_dt = @END_DATE
 WHERE end_dt > @END_DATE

IF @DEBUG = 1
BEGIN
  SELECT '#UNIVERSE_DATES'
  SELECT * FROM #UNIVERSE_DATES ORDER BY begin_dt, universe_id
END

CREATE TABLE #MQA_ID_DATES (
  begin_dt	datetime	NOT NULL,
  end_dt	datetime	NOT NULL,
  SecurityId	int		NULL,
  mqa_id	varchar(32)	NULL,
  ticker	varchar(16)	NULL,
  cusip		varchar(32)	NULL,
  sedol		varchar(32)	NULL,
  isin		varchar(64)	NULL,
  gv_key	int		NULL
)

INSERT #MQA_ID_DATES
SELECT DISTINCT u.begin_dt, u.end_dt, NULL, p.mqa_id, p.ticker, p.cusip, p.sedol, p.isin, p.gv_key
  FROM #UNIVERSE_DATES u, QER..universe_makeup p
 WHERE u.universe_dt = p.universe_dt
   AND u.universe_id = p.universe_id

UPDATE #MQA_ID_DATES
   SET SecurityId = c.Id
  FROM qmetrix..tblSecurity c,
       (SELECT d.mqa_id, max(EffDate) AS EffDate
          FROM #MQA_ID_DATES d, qmetrix..tblSecurity s
         WHERE d.mqa_id = s.QAId
           AND d.end_dt >= s.EffDate
         GROUP BY d.mqa_id) x
 WHERE #MQA_ID_DATES.mqa_id = c.QAId
   AND c.QAId = x.mqa_id
   AND c.EffDate = x.EffDate

IF @DEBUG = 1
BEGIN
  SELECT '#MQA_ID_DATES'
  SELECT * FROM #MQA_ID_DATES ORDER BY begin_dt, mqa_id
END

CREATE TABLE #INSTRUMENT_FACTOR_STAGING (
  bdate		datetime	NULL,
  mqa_id	varchar(32)	NULL,
  ticker	varchar(16)	NULL,
  cusip		varchar(32)	NULL,
  sedol		varchar(32)	NULL,
  isin		varchar(64)	NULL,
  gv_key	int		NULL,
  factor_id	int		NULL,
  factor_value	float		NULL,
  source_cd	varchar(8)	NULL
)

INSERT #INSTRUMENT_FACTOR_STAGING
SELECT f.TransDate, d.mqa_id, d.ticker, d.cusip, d.sedol, d.isin, d.gv_key, m.qer_factor_id, f.NumValue,
       CASE WHEN dsd.Id = 1 THEN 'FS'
            WHEN dsd.Id IN (2,3) THEN 'MQA'
            ELSE NULL
       END
  FROM #MQA_ID_DATES d, QER..factor_map m, qmetrix..tblSecurityFactors f,
       qmetrix..tblFactorDescriptors fd, qmetrix..tblDataSourceDescriptors dsd
 WHERE d.SecurityId = f.SecurityId
   AND f.TransDate >= d.begin_dt
   AND f.TransDate <= d.end_dt
   AND m.qmetrix_factor_id = f.FactorId
   AND m.qmetrix_factor_id = fd.Id
   AND fd.DataSourceId = dsd.Id

CREATE TABLE #INSTRUMENT_FACTOR_STAGING2 (
  bdate		datetime	NULL,
  mqa_id	varchar(32)	NULL,
  ticker	varchar(16)	NULL,
  cusip		varchar(32)	NULL,
  sedol		varchar(32)	NULL,
  isin		varchar(64)	NULL,
  gv_key	int		NULL,
  factor_id	int		NULL,
  factor_value	float		NULL,
  source_cd	varchar(8)	NULL
)

INSERT #INSTRUMENT_FACTOR_STAGING2
SELECT DISTINCT bdate, mqa_id, ticker, cusip, sedol, isin, gv_key, factor_id, factor_value, source_cd
  FROM #INSTRUMENT_FACTOR_STAGING

IF @DEBUG = 1
BEGIN
  SELECT '#INSTRUMENT_FACTOR_STAGING ROWCOUNT = ', count(*) FROM #INSTRUMENT_FACTOR_STAGING
  SELECT '#INSTRUMENT_FACTOR_STAGING2 ROWCOUNT = ', count(*) FROM #INSTRUMENT_FACTOR_STAGING2
END
/*
DELETE #INSTRUMENT_FACTOR_STAGING2
  FROM instrument_factor i
 WHERE #INSTRUMENT_FACTOR_STAGING2.bdate = i.bdate
   AND #INSTRUMENT_FACTOR_STAGING2.mqa_id = i.mqa_id
   AND #INSTRUMENT_FACTOR_STAGING2.factor_id = i.factor_id
   AND #INSTRUMENT_FACTOR_STAGING2.factor_value = i.factor_value
   AND #INSTRUMENT_FACTOR_STAGING2.source_cd = i.source_cd

DELETE #INSTRUMENT_FACTOR_STAGING2
  FROM instrument_factor i
 WHERE #INSTRUMENT_FACTOR_STAGING2.bdate = i.bdate
   AND (#INSTRUMENT_FACTOR_STAGING2.mqa_id IS NULL OR i.mqa_id IS NULL)
   AND #INSTRUMENT_FACTOR_STAGING2.cusip = i.cusip
   AND #INSTRUMENT_FACTOR_STAGING2.factor_id = i.factor_id
   AND #INSTRUMENT_FACTOR_STAGING2.factor_value = i.factor_value
   AND #INSTRUMENT_FACTOR_STAGING2.source_cd = i.source_cd
*/
IF @DEBUG = 1
BEGIN
  SELECT '#INSTRUMENT_FACTOR_STAGING2'
  SELECT * FROM #INSTRUMENT_FACTOR_STAGING2 ORDER BY bdate, mqa_id, factor_id
END

INSERT instrument_factor
      (bdate, mqa_id, ticker, cusip, sedol, isin, gv_key,
       factor_id, factor_value, update_tm, source_cd)
SELECT bdate, mqa_id, ticker, cusip, sedol, isin, gv_key,
       factor_id, factor_value, bdate, source_cd
  FROM #INSTRUMENT_FACTOR_STAGING2

RETURN 0
go
IF OBJECT_ID('dbo.factor_import_qmetrix') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.factor_import_qmetrix >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.factor_import_qmetrix >>>'
go