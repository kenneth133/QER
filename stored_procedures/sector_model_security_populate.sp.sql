use QER
go

IF OBJECT_ID('dbo.sector_model_security_populate') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.sector_model_security_populate
    IF OBJECT_ID('dbo.sector_model_security_populate') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.sector_model_security_populate >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.sector_model_security_populate >>>'
END
go
CREATE PROCEDURE dbo.sector_model_security_populate
@BDATE datetime,
@SECTOR_MODEL_ID int,
@UNIVERSE_DT datetime = NULL,
@UNIVERSE_CD varchar(32) = NULL,
@UNIVERSE_ID int = NULL,
@DEBUG bit = NULL
AS

/****
* NOTE:
* IF @UNIVERSE_ID IS NULL AND @UNIVERSE_DT IS NULL
*   THIS PROCEDURE WILL CLASSIFY ALL SECURITIES ACCORDING TO @SECTOR_MODEL_ID
*   FROM INSTRUMENT_CHARACTERISTICS WHERE BDATE = @BDATE
****/

IF @BDATE IS NULL
  BEGIN SELECT 'ERROR: @BDATE IS A REQUIRED PARAMETER' RETURN -1 END
IF @SECTOR_MODEL_ID IS NULL
  BEGIN SELECT 'ERROR: @SECTOR_MODEL_ID IS A REQUIRED PARAMETER' RETURN -1 END

IF @UNIVERSE_DT IS NULL
  BEGIN SELECT @UNIVERSE_DT = @BDATE END

IF @UNIVERSE_CD IS NOT NULL
BEGIN
  SELECT @UNIVERSE_ID = universe_id
    FROM universe_def
   WHERE universe_cd = @UNIVERSE_CD
END

CREATE TABLE #SEC (
  security_id			int		NULL,
  gics_sub_industry_num	int		NULL,
  russell_industry_num	int		NULL
)

IF @UNIVERSE_CD IS NOT NULL AND EXISTS (SELECT 1 FROM benchmark WHERE benchmark_cd=@UNIVERSE_CD)
BEGIN
  INSERT #SEC
  SELECT DISTINCT y.security_id, y.gics_sub_industry_num, y.russell_industry_num
    FROM equity_common..security y, equity_common..position p
   WHERE p.reference_date = @UNIVERSE_DT
     AND p.reference_date = p.effective_date
     AND p.acct_cd IN (SELECT DISTINCT acct_cd FROM equity_common..account WHERE parent = @UNIVERSE_CD OR acct_cd = @UNIVERSE_CD)
     AND p.security_id = y.security_id
     AND p.security_id IS NOT NULL
     AND NOT EXISTS (SELECT 1 FROM sector_model_security ss
                      WHERE ss.bdate = @BDATE
                        AND ss.sector_model_id = @SECTOR_MODEL_ID
                        AND ss.security_id = p.security_id
                        AND ss.security_id IS NOT NULL)
END
ELSE IF @UNIVERSE_ID IS NOT NULL
BEGIN
  INSERT #SEC
  SELECT DISTINCT y.security_id, y.gics_sub_industry_num, y.russell_industry_num
    FROM equity_common..security y, universe_makeup p
   WHERE p.universe_dt = @UNIVERSE_DT
     AND p.universe_id = @UNIVERSE_ID
     AND p.security_id = y.security_id
     AND p.security_id IS NOT NULL
     AND NOT EXISTS (SELECT 1 FROM sector_model_security ss
                      WHERE ss.bdate = @BDATE
                        AND ss.sector_model_id = @SECTOR_MODEL_ID
                        AND ss.security_id = p.security_id
                        AND ss.security_id IS NOT NULL)
END
ELSE
BEGIN
  INSERT #SEC
  SELECT DISTINCT y.security_id, y.gics_sub_industry_num, y.russell_industry_num
    FROM equity_common..security y,
        (SELECT security_id FROM universe_makeup WHERE universe_dt = @UNIVERSE_DT AND security_id IS NOT NULL
          UNION
         SELECT security_id FROM equity_common..position
          WHERE reference_date = @UNIVERSE_DT
            AND reference_date = effective_date
            AND acct_cd IN (SELECT DISTINCT a.acct_cd
                              FROM equity_common..account a, benchmark b
                             WHERE a.parent = b.benchmark_cd OR a.acct_cd = b.benchmark_cd)
            AND security_id IS NOT NULL) x
   WHERE x.security_id = y.security_id
     AND NOT EXISTS (SELECT 1 FROM sector_model_security ss
                      WHERE ss.bdate = @BDATE
                        AND ss.sector_model_id = @SECTOR_MODEL_ID
                        AND ss.security_id = x.security_id
                        AND ss.security_id IS NOT NULL)
END

IF @DEBUG = 1
BEGIN
  SELECT '#SEC: AFTER INITIAL INSERT'
  SELECT * FROM #SEC ORDER BY security_id
END

IF NOT EXISTS (SELECT * FROM #SEC)
BEGIN
  DROP TABLE #SEC
  RETURN 0
END

CREATE TABLE #NODE (
  sector_id		int			NOT NULL,
  segment_id	int			NULL,
  child_type	varchar(1)	NOT NULL,
  child_id		int			NOT NULL
)

INSERT #NODE
SELECT g.sector_id, g.segment_id, p.segment_child_type, p.segment_child_id
  FROM sector_def c, segment_def g, segment_makeup p
 WHERE c.sector_model_id = @SECTOR_MODEL_ID
   AND c.sector_id = g.sector_id
   AND g.segment_id = p.segment_id

INSERT #NODE
SELECT d.sector_id, NULL, p.sector_child_type, p.sector_child_id
  FROM sector_def d, sector_makeup p
 WHERE d.sector_model_id = @SECTOR_MODEL_ID
   AND d.sector_id = p.sector_id
   AND p.sector_child_type = 'G'
   AND p.sector_child_id NOT IN (SELECT segment_id FROM #NODE)

INSERT #NODE
SELECT d.sector_id, NULL, p.sector_child_type, p.sector_child_id
  FROM sector_def d, sector_makeup p
 WHERE d.sector_model_id = @SECTOR_MODEL_ID
   AND d.sector_id = p.sector_id
   AND p.sector_child_type != 'G'

IF @DEBUG = 1
BEGIN
  SELECT '#NODE: AFTER INITIAL INSERTS'
  SELECT * FROM #NODE ORDER BY sector_id, segment_id, child_type, child_id
END

CREATE TABLE #RUSSELL_INDUSTRY (
  industry_id	int	NOT NULL,
  industry_num	int	NOT NULL
)

CREATE TABLE #GICS_SUB_INDUSTRY (
  sub_industry_num	int	NOT NULL
)

INSERT #RUSSELL_INDUSTRY
SELECT i.industry_id, i.industry_num
  FROM industry_model m, industry i
 WHERE m.industry_model_cd = 'RUSSELL-I'
   AND m.industry_model_id = i.industry_model_id

INSERT #GICS_SUB_INDUSTRY
SELECT b.sub_industry_num
  FROM industry_model m, industry i, sub_industry b
 WHERE m.industry_model_cd = 'GICS-I'
   AND m.industry_model_id = i.industry_model_id
   AND i.industry_id = b.industry_id

IF @DEBUG = 1
BEGIN
  SELECT '#RUSSELL_INDUSTRY'
  SELECT * FROM #RUSSELL_INDUSTRY ORDER BY industry_num

  SELECT '#GICS_SUB_INDUSTRY'
  SELECT * FROM #GICS_SUB_INDUSTRY ORDER BY sub_industry_num
END

CREATE TABLE #LEAF (
  sector_id	int		NULL,
  segment_id	int		NULL,
  child_type	varchar(1)	NULL,
  child_id	int		NULL,
  child_num	int		NULL
)

CREATE TABLE #TWIG (
  sector_id	int		NULL,
  segment_id	int		NULL,
  child_type	varchar(1)	NULL,
  child_id	int		NULL
)

WHILE EXISTS (SELECT * FROM #NODE)
BEGIN
  INSERT #LEAF
  SELECT n.sector_id, n.segment_id, n.child_type, n.child_id, i.industry_num
    FROM #NODE n, #RUSSELL_INDUSTRY i
   WHERE n.child_type = 'I'
     AND n.child_id = i.industry_id
  UNION
  SELECT n.sector_id, n.segment_id, n.child_type, n.child_id, b.sub_industry_num
    FROM #NODE n, #GICS_SUB_INDUSTRY b
   WHERE n.child_type = 'B'
     AND n.child_id = b.sub_industry_num

  DELETE #NODE
    FROM #LEAF l
   WHERE #NODE.sector_id = l.sector_id
     AND #NODE.segment_id = l.segment_id
     AND #NODE.child_type = l.child_type
     AND #NODE.child_id = l.child_id

  DELETE #TWIG
  INSERT #TWIG
  SELECT sector_id, segment_id, child_type, child_id
    FROM #NODE

  DELETE #NODE

  INSERT #NODE
  SELECT t.sector_id, t.segment_id, p.sector_child_type, p.sector_child_id
    FROM #TWIG t, sector_makeup p
   WHERE t.child_type = 'C'
     AND p.sector_id = t.child_id
  UNION
  SELECT t.sector_id, t.segment_id, p.segment_child_type, p.segment_child_id
    FROM #TWIG t, segment_makeup p
   WHERE t.child_type = 'G'
     AND p.segment_id = t.child_id
  UNION
  SELECT t.sector_id, t.segment_id, 'B', b.sub_industry_num
    FROM #TWIG t, sub_industry b
   WHERE t.child_type = 'I'
     AND b.industry_id = t.child_id
END

IF @DEBUG = 1
BEGIN
  SELECT '#LEAF: AFTER LOOP'
  SELECT * FROM #LEAF ORDER BY sector_id, segment_id, child_type, child_id
END

DROP TABLE #TWIG
DROP TABLE #NODE

--DELETE GICS_SUB_INDUSTRY_NUM FROM A SECTOR (1): BEGIN
DELETE #LEAF
  FROM sector_model_map p
 WHERE p.sector_model_id = @SECTOR_MODEL_ID
   AND p.map_id = 3
   AND #LEAF.sector_id = p.sector_id
   AND #LEAF.child_type = 'B'
   AND #LEAF.child_num = p.gics_sub_industry_num
--DELETE GICS_SUB_INDUSTRY_NUM FROM A SECTOR (1): END

--GICS_SUB_INDUSTRY TO RUSSELL_INDUSTRY WHERE RUSSELL_INDUSTRY IS NULL: BEGIN
IF EXISTS (SELECT * FROM sector_model_map
            WHERE sector_model_id = @SECTOR_MODEL_ID
              AND map_id = 1)
BEGIN
  UPDATE #SEC
     SET russell_industry_num = m.russell_industry_num
    FROM sector_model_map m
   WHERE m.sector_model_id = @SECTOR_MODEL_ID
     AND m.map_id = 1
     AND #SEC.gics_sub_industry_num = m.gics_sub_industry_num
     AND #SEC.russell_industry_num IS NULL

  IF @DEBUG = 1
  BEGIN
    SELECT '#SEC: AFTER MAPPING GICS_SUB_INDUSTRY TO RUSSELL_INDUSTRY WHERE RUSSELL_INDUSTRY IS NULL'
    SELECT * FROM #SEC ORDER BY security_id
  END
END
--GICS_SUB_INDUSTRY TO RUSSELL_INDUSTRY WHERE RUSSELL_INDUSTRY IS NULL: END

--RUSSELL_INDUSTRY TO GICS_SUB_INDUSTRY WHERE GICS_SUB_INDUSTRY IS NULL: BEGIN
IF EXISTS (SELECT * FROM sector_model_map
            WHERE sector_model_id = @SECTOR_MODEL_ID
              AND map_id = 2)
BEGIN
  UPDATE #SEC
     SET gics_sub_industry_num = m.gics_sub_industry_num
    FROM sector_model_map m
   WHERE m.sector_model_id = @SECTOR_MODEL_ID
     AND m.map_id = 2
     AND #SEC.russell_industry_num = m.russell_industry_num
     AND #SEC.gics_sub_industry_num IS NULL

  IF @DEBUG = 1
  BEGIN
    SELECT '#SEC: AFTER MAPPING RUSSELL_INDUSTRY TO GICS_SUB_INDUSTRY WHERE GICS_SUB_INDUSTRY IS NULL'
    SELECT * FROM #SEC ORDER BY security_id
  END
END
--RUSSELL_INDUSTRY TO GICS_SUB_INDUSTRY WHERE GICS_SUB_INDUSTRY IS NULL: END

--DELETE GICS_SUB_INDUSTRY_NUM FROM A SECTOR (2): BEGIN
DELETE #LEAF
  FROM sector_model_map p
 WHERE p.sector_model_id = @SECTOR_MODEL_ID
   AND p.map_id = 3
   AND #LEAF.sector_id = p.sector_id
   AND #LEAF.child_type = 'B'
   AND #LEAF.child_num = p.gics_sub_industry_num
--DELETE GICS_SUB_INDUSTRY_NUM FROM A SECTOR (2): END

INSERT sector_model_security
SELECT @BDATE, @SECTOR_MODEL_ID, l.sector_id, l.segment_id, s.security_id
  FROM #LEAF l, #SEC s
 WHERE l.child_type = 'I'
   AND l.child_num = s.russell_industry_num
UNION
SELECT @BDATE, @SECTOR_MODEL_ID, l.sector_id, l.segment_id, s.security_id
  FROM #LEAF l, #SEC s
 WHERE l.child_type = 'B'
   AND l.child_num = s.gics_sub_industry_num

IF @DEBUG = 1
BEGIN
  SELECT 'sector_model_security (1) AFTER INSERT'
  SELECT * FROM sector_model_security
   WHERE bdate = @BDATE
     AND sector_model_id = @SECTOR_MODEL_ID
   ORDER BY security_id
END

--SECURITY EXCEPTIONS LOGIC (WITH NO OVERRIDE): BEGIN
IF EXISTS (SELECT * FROM sector_model_security_exception
            WHERE sector_model_id = @SECTOR_MODEL_ID
              AND override != 1)
BEGIN
  INSERT sector_model_security
  SELECT DISTINCT @BDATE, @SECTOR_MODEL_ID, x.sector_id, x.segment_id, s.security_id
    FROM #SEC s, sector_model_security_exception x
   WHERE x.sector_model_id = @SECTOR_MODEL_ID
     AND x.override != 1
     AND s.security_id = x.security_id
     AND x.security_id IS NOT NULL
     AND NOT EXISTS (SELECT 1 FROM sector_model_security ss
                      WHERE ss.bdate = @BDATE
                        AND ss.sector_model_id = @SECTOR_MODEL_ID
                        AND ss.security_id = x.security_id
                        AND ss.security_id IS NOT NULL)

  IF @DEBUG = 1
  BEGIN
    SELECT 'sector_model_security (2) AFTER INSERT'
    SELECT * FROM sector_model_security
     WHERE bdate = @BDATE
       AND sector_model_id = @SECTOR_MODEL_ID
     ORDER BY security_id
  END
END
--SECURITY EXCEPTIONS LOGIC (WITH NO OVERRIDE): END

INSERT sector_model_security
SELECT DISTINCT @BDATE, @SECTOR_MODEL_ID, NULL, NULL, s.security_id
  FROM #SEC s
 WHERE s.security_id IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sector_model_security ss
                    WHERE ss.bdate = @BDATE
                      AND ss.sector_model_id = @SECTOR_MODEL_ID
                      AND ss.security_id = s.security_id
                      AND ss.security_id IS NOT NULL)

DROP TABLE #SEC
DROP TABLE #LEAF

IF @DEBUG = 1
BEGIN
  SELECT 'sector_model_security (3) AFTER INSERT'
  SELECT * FROM sector_model_security
   WHERE bdate = @BDATE
     AND sector_model_id = @SECTOR_MODEL_ID
   ORDER BY security_id
END

--SECURITY EXCEPTIONS LOGIC (WITH OVERRIDE): BEGIN
IF EXISTS (SELECT * FROM sector_model_security_exception
            WHERE sector_model_id = @SECTOR_MODEL_ID
              AND override = 1)
BEGIN
  UPDATE sector_model_security
     SET sector_id = x.sector_id,
         segment_id = x.segment_id
    FROM sector_model_security_exception x
   WHERE sector_model_security.bdate = @BDATE
     AND sector_model_security.sector_model_id = @SECTOR_MODEL_ID
     AND sector_model_security.sector_model_id = x.sector_model_id
     AND x.override = 1
     AND sector_model_security.security_id = x.security_id

  IF @DEBUG = 1
  BEGIN
    SELECT 'sector_model_security (4) AFTER UPDATE'
    SELECT * FROM sector_model_security
     WHERE bdate = @BDATE
       AND sector_model_id = @SECTOR_MODEL_ID
     ORDER BY security_id
  END
END
--SECURITY EXCEPTIONS LOGIC (WITH OVERRIDE): END

RETURN 0
go
IF OBJECT_ID('dbo.sector_model_security_populate') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.sector_model_security_populate >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.sector_model_security_populate >>>'
go
