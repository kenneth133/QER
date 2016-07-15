use QER
go
IF OBJECT_ID('dbo.msci_benchmark_weights_compute') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.msci_benchmark_weights_compute
    IF OBJECT_ID('dbo.msci_benchmark_weights_compute') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.msci_benchmark_weights_compute >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.msci_benchmark_weights_compute >>>'
END
go
CREATE PROCEDURE dbo.msci_benchmark_weights_compute @STARTDATE datetime = NULL,
                                                    @ENDDATE datetime = NULL
AS

IF @STARTDATE IS NULL
BEGIN
  SELECT 'ERROR: @STARTDATE CANNOT BE NULL'
  RETURN -1
END

IF @ENDDATE IS NULL
BEGIN
  SELECT 'ERROR: @ENDDATE CANNOT BE NULL'
  RETURN -1
END

IF @STARTDATE > @ENDDATE
BEGIN
  SELECT 'ERROR: @STARTDATE CANNOT BE LATER THAN @ENDDATE'
  RETURN -1
END

CREATE TABLE #UNIVERSE_MKTCAP (
  universe_dt		datetime	NOT NULL,
  universe_id		int		NOT NULL,
  universe_mktcap	float		NULL
)

CREATE TABLE #SECURITY_MKTCAP (
  bdate		datetime	NOT NULL,
  mqa_id	varchar(32)	NOT NULL,
  mktcap_adj	float		NULL
)

INSERT #SECURITY_MKTCAP
SELECT DISTINCT c.bdate, c.mqa_id, ISNULL(c.mktcap,0) * ISNULL(i.factor_value,1)
  FROM universe_def d, universe_makeup m,
       factor f, instrument_factor i, instrument_characteristics c
 WHERE d.universe_cd LIKE 'MSCI%'
   AND d.universe_id = m.universe_id
   AND m.universe_dt >= @STARTDATE
   AND m.universe_dt <= @ENDDATE
   AND m.universe_dt = c.bdate
   AND m.universe_dt = i.bdate
   AND m.mqa_id = c.mqa_id
   AND m.mqa_id = i.mqa_id
   AND i.factor_id = f.factor_id
   AND f.factor_cd = 'MSCI_FIF'
 ORDER BY c.bdate, c.mqa_id

INSERT #UNIVERSE_MKTCAP
SELECT m.universe_dt, m.universe_id, SUM(s.mktcap_adj)
  FROM #SECURITY_MKTCAP s, universe_def d, universe_makeup m
 WHERE d.universe_cd LIKE 'MSCI%'
   AND d.universe_id = m.universe_id
   AND m.universe_dt = s.bdate
   AND m.mqa_id = s.mqa_id
 GROUP BY m.universe_dt, m.universe_id

UPDATE universe_makeup
   SET weight = s.mktcap_adj / u.universe_mktcap
  FROM #SECURITY_MKTCAP s, #UNIVERSE_MKTCAP u
 WHERE universe_makeup.universe_dt = u.universe_dt
   AND universe_makeup.universe_id = u.universe_id
   AND universe_makeup.universe_dt = s.bdate   
   AND universe_makeup.mqa_id = s.mqa_id

DROP TABLE #UNIVERSE_MKTCAP
DROP TABLE #SECURITY_MKTCAP

RETURN 0
go
IF OBJECT_ID('dbo.msci_benchmark_weights_compute') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.msci_benchmark_weights_compute >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.msci_benchmark_weights_compute >>>'
go