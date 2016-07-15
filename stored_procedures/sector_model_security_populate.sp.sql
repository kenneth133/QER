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
CREATE PROCEDURE dbo.sector_model_security_populate @BDATE datetime,
                                                    @SECTOR_MODEL_ID int,
                                                    @UNIVERSE_DT datetime = NULL,
                                                    @UNIVERSE_ID int =NULL,
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

CREATE TABLE #SEC (
  mqa_id		varchar(32)	NULL,
  ticker		varchar(16)	NULL,
  cusip			varchar(32)	NULL,
  sedol			varchar(32)	NULL,
  isin			varchar(64)	NULL,
  gv_key		int		NULL,
  gics_sub_industry_num	int		NULL,
  russell_industry_num	int		NULL
)

IF @UNIVERSE_ID IS NOT NULL AND @UNIVERSE_DT IS NOT NULL
BEGIN
  INSERT #SEC
  SELECT DISTINCT i.mqa_id, i.ticker, i.cusip, i.sedol, i.isin, i.gv_key, i.gics_sub_industry_num, i.russell_industry_num
    FROM instrument_characteristics i, universe_makeup u
   WHERE i.bdate = @BDATE
     AND u.universe_dt = @UNIVERSE_DT
     AND u.universe_id = @UNIVERSE_ID
     AND i.cusip = u.cusip
     AND u.cusip IS NOT NULL
     AND u.cusip NOT IN (SELECT cusip FROM sector_model_security
                          WHERE bdate = @BDATE
                            AND sector_model_id = @SECTOR_MODEL_ID
                            AND cusip IS NOT NULL)
END
ELSE
BEGIN
  INSERT #SEC
  SELECT DISTINCT i.mqa_id, i.ticker, i.cusip, i.sedol, i.isin, i.gv_key, i.gics_sub_industry_num, i.russell_industry_num
    FROM instrument_characteristics i
   WHERE i.bdate = @BDATE
     AND i.cusip IS NOT NULL
     AND i.cusip NOT IN (SELECT cusip FROM sector_model_security
                          WHERE bdate = @BDATE
                            AND sector_model_id = @SECTOR_MODEL_ID
                            AND cusip IS NOT NULL)
END

IF @DEBUG = 1
BEGIN
  SELECT '#SEC: AFTER INITIAL INSERT'
  SELECT * FROM #SEC ORDER BY ticker, cusip, sedol, isin
END

IF NOT EXISTS (SELECT * FROM #SEC)
BEGIN
  DROP TABLE #SEC
  RETURN 0
END

CREATE TABLE #NODE (
  sector_id	int		NOT NULL,
  segment_id	int		NULL,
  child_type	varchar(1)	NOT NULL,
  child_id	int		NOT NULL
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
    SELECT * FROM #SEC ORDER BY ticker, cusip, sedol, isin
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
    SELECT * FROM #SEC ORDER BY ticker, cusip, sedol, isin
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
SELECT @BDATE, @SECTOR_MODEL_ID, l.sector_id, l.segment_id,
       s.mqa_id, s.ticker, s.cusip, s.sedol, s.isin, s.gv_key
  FROM #LEAF l, #SEC s
 WHERE l.child_type = 'I'
   AND l.child_num = s.russell_industry_num
UNION
SELECT @BDATE, @SECTOR_MODEL_ID, l.sector_id, l.segment_id,
       s.mqa_id, s.ticker, s.cusip, s.sedol, s.isin, s.gv_key
  FROM #LEAF l, #SEC s
 WHERE l.child_type = 'B'
   AND l.child_num = s.gics_sub_industry_num

IF @DEBUG = 1
BEGIN
  SELECT 'QER..sector_model_security (1) AFTER INSERT'
  SELECT * FROM sector_model_security
   WHERE bdate = @BDATE
     AND sector_model_id = @SECTOR_MODEL_ID
   ORDER BY ticker, cusip, sedol, isin
END

--SECURITY EXCEPTIONS LOGIC (WITH NO OVERRIDE): BEGIN
IF EXISTS (SELECT * FROM sector_model_security_exception
            WHERE sector_model_id = @SECTOR_MODEL_ID
              AND override != 1)
BEGIN
  INSERT sector_model_security
  SELECT DISTINCT @BDATE, @SECTOR_MODEL_ID, x.sector_id, x.segment_id,
         s.mqa_id, s.ticker, s.cusip, s.sedol, s.isin, s.gv_key
    FROM #SEC s, sector_model_security_exception x
   WHERE x.sector_model_id = @SECTOR_MODEL_ID
     AND x.override != 1
     AND s.cusip = x.cusip
     AND x.cusip IS NOT NULL
     AND x.cusip NOT IN (SELECT cusip FROM sector_model_security
                          WHERE bdate = @BDATE
                            AND sector_model_id = @SECTOR_MODEL_ID
                            AND cusip IS NOT NULL)

  IF @DEBUG = 1
  BEGIN
    SELECT 'QER..sector_model_security (2) AFTER INSERT'
    SELECT * FROM sector_model_security
     WHERE bdate = @BDATE
       AND sector_model_id = @SECTOR_MODEL_ID
     ORDER BY ticker, cusip, sedol, isin
  END
END
--SECURITY EXCEPTIONS LOGIC (WITH NO OVERRIDE): END

INSERT sector_model_security
SELECT DISTINCT @BDATE, @SECTOR_MODEL_ID, NULL, NULL,
       mqa_id, ticker, cusip, sedol, isin, gv_key
  FROM #SEC
 WHERE cusip IS NOT NULL
   AND cusip NOT IN (SELECT cusip FROM sector_model_security
                          WHERE bdate = @BDATE
                            AND sector_model_id = @SECTOR_MODEL_ID
                            AND cusip IS NOT NULL)

DROP TABLE #SEC
DROP TABLE #LEAF

IF @DEBUG = 1
BEGIN
  SELECT 'QER..sector_model_security (3) AFTER INSERT'
  SELECT * FROM sector_model_security
   WHERE bdate = @BDATE
     AND sector_model_id = @SECTOR_MODEL_ID
   ORDER BY ticker, cusip, sedol, isin
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
     AND sector_model_security.cusip = x.cusip

  IF @DEBUG = 1
  BEGIN
    SELECT 'QER..sector_model_security (4) AFTER UPDATE'
    SELECT * FROM sector_model_security
     WHERE bdate = @BDATE
       AND sector_model_id = @SECTOR_MODEL_ID
     ORDER BY ticker, cusip, sedol, isin
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
