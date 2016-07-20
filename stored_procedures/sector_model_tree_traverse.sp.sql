use QER
go

CREATE TABLE #RUSSELL_INDUSTRY (
  industry_id	int	NOT NULL,
  industry_num	int	NOT NULL
)

CREATE TABLE #GICS_SUB_INDUSTRY (
  sub_industry_num	int	NOT NULL
)

CREATE TABLE #NODE (
  sector_id		int			NOT NULL,
  segment_id	int			NULL,
  child_type	varchar(1)	NOT NULL,
  child_id		int			NOT NULL
)

CREATE TABLE #LEAF (
  sector_id		int			NULL,
  segment_id	int			NULL,
  child_type	varchar(1)	NULL,
  child_id		int			NULL,
  child_num		int			NULL
)

IF OBJECT_ID('dbo.sector_model_tree_traverse') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.sector_model_tree_traverse
    IF OBJECT_ID('dbo.sector_model_tree_traverse') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.sector_model_tree_traverse >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.sector_model_tree_traverse >>>'
END
go
CREATE PROCEDURE dbo.sector_model_tree_traverse
@DEBUG bit = NULL
AS

CREATE TABLE #TEMP (
  sector_id		int			NULL,
  segment_id	int			NULL,
  child_type	varchar(1)	NULL,
  child_id		int			NULL
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

  DELETE #TEMP

  INSERT #TEMP
  SELECT sector_id, segment_id, child_type, child_id
    FROM #NODE

  DELETE #NODE

  INSERT #NODE
  SELECT t.sector_id, t.segment_id, p.sector_child_type, p.sector_child_id
    FROM #TEMP t, sector_makeup p
   WHERE t.child_type = 'C'
     AND p.sector_id = t.child_id
  UNION
  SELECT t.sector_id, t.segment_id, p.segment_child_type, p.segment_child_id
    FROM #TEMP t, segment_makeup p
   WHERE t.child_type = 'G'
     AND p.segment_id = t.child_id
  UNION
  SELECT t.sector_id, t.segment_id, 'B', b.sub_industry_num
    FROM #TEMP t, sub_industry b
   WHERE t.child_type = 'I'
     AND b.industry_id = t.child_id
END

DROP TABLE #TEMP

IF @DEBUG = 1
BEGIN
  SELECT '#LEAF'
  SELECT * FROM #LEAF ORDER BY sector_id, segment_id, child_type, child_id
END

RETURN 0
go
IF OBJECT_ID('dbo.sector_model_tree_traverse') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.sector_model_tree_traverse >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.sector_model_tree_traverse >>>'
go

DROP TABLE #LEAF
DROP TABLE #NODE
DROP TABLE #GICS_SUB_INDUSTRY
DROP TABLE #RUSSELL_INDUSTRY
