use QER
go
IF OBJECT_ID('dbo.strategy_scores_compute') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.strategy_scores_compute
    IF OBJECT_ID('dbo.strategy_scores_compute') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.strategy_scores_compute >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.strategy_scores_compute >>>'
END
go
CREATE PROCEDURE dbo.strategy_scores_compute
@BDATE datetime = NULL, --optional, defaults to previous business day
@STRATEGY_ID int, --required
@DEBUG bit = NULL --optional, for debugging
AS

IF @STRATEGY_ID IS NULL
BEGIN
  SELECT 'ERROR: @STRATEGY_ID PARAMETER MUST BE PASSED'
  RETURN -1
END

IF @BDATE IS NULL
BEGIN
  EXEC business_date_get @DIFF=-1, @RET_DATE=@BDATE OUTPUT
END

DECLARE @FACTOR_MODEL_ID	int,
        @SECTOR_MODEL_ID	int,
        @UNIVERSE_ID		int,
        @GROUPS				int

SELECT @FACTOR_MODEL_ID = s.factor_model_id,
       @SECTOR_MODEL_ID = f.sector_model_id,
       @UNIVERSE_ID = s.universe_id,
       @GROUPS = s.fractile
  FROM strategy s, factor_model f
 WHERE s.strategy_id = @STRATEGY_ID
   AND s.factor_model_id = f.factor_model_id

IF @DEBUG = 1
BEGIN
  SELECT '@FACTOR_MODEL_ID', @FACTOR_MODEL_ID
  SELECT '@SECTOR_MODEL_ID', @SECTOR_MODEL_ID
  SELECT '@UNIVERSE_ID', @UNIVERSE_ID
  SELECT '@GROUPS', @GROUPS
END

CREATE TABLE #RANK_PARAMETERS (
  rank_event_id	int			NULL,
  bdate			datetime	NOT NULL,
  universe_dt	datetime	NOT NULL,
  universe_id	int			NOT NULL,
  factor_id		int			NOT NULL,
  against		varchar(1)	NOT NULL,
  against_cd	varchar(8)	NULL,
  against_id	int			NULL,
  weight		float		NULL
)

INSERT #RANK_PARAMETERS (rank_event_id, bdate, universe_dt, universe_id, factor_id, against, weight)
SELECT i.rank_event_id, i.bdate, i.universe_dt, i.universe_id, w.factor_id, w.against, w.weight
  FROM factor_against_weight w, rank_inputs i
 WHERE w.factor_model_id = @FACTOR_MODEL_ID
   AND w.factor_id = i.factor_id
   AND w.against = i.against
   AND w.against = 'U'
   AND i.bdate = @BDATE
   AND i.universe_id = @UNIVERSE_ID
   AND i.groups = @GROUPS

INSERT #RANK_PARAMETERS (rank_event_id, bdate, universe_dt, universe_id, factor_id, against, against_id, weight)
SELECT i.rank_event_id, i.bdate, i.universe_dt, i.universe_id, w.factor_id, w.against, w.against_id, w.weight
  FROM factor_against_weight w, rank_inputs i
 WHERE w.factor_model_id = @FACTOR_MODEL_ID
   AND w.factor_id = i.factor_id
   AND w.against = i.against
   AND w.against IN ('C','G')
   AND w.against_id = i.against_id
   AND i.bdate = @BDATE
   AND i.universe_id = @UNIVERSE_ID
   AND i.groups = @GROUPS

INSERT #RANK_PARAMETERS (rank_event_id, bdate, universe_dt, universe_id, factor_id, against, against_cd, weight)
SELECT i.rank_event_id, i.bdate, i.universe_dt, i.universe_id, w.factor_id, w.against, i.against_cd, w.weight
  FROM factor_against_weight w, rank_inputs i
 WHERE w.factor_model_id = @FACTOR_MODEL_ID
   AND w.factor_id = i.factor_id
   AND w.against = i.against
   AND w.against = 'Y'
   AND i.bdate = @BDATE
   AND i.universe_id = @UNIVERSE_ID
   AND i.groups = @GROUPS

IF @DEBUG = 1
BEGIN
  SELECT '#RANK_PARAMETERS'
  SELECT * FROM #RANK_PARAMETERS ORDER BY rank_event_id
END

CREATE TABLE #SECURITY_CLASS (
  security_id	int			NULL,
  sector_id		int			NULL,
  segment_id	int			NULL,
  country_cd	varchar(8)	NULL
)

INSERT #SECURITY_CLASS (security_id, sector_id, segment_id)
SELECT security_id, sector_id, segment_id
  FROM sector_model_security
 WHERE bdate = @BDATE
   AND sector_model_id = @SECTOR_MODEL_ID
   AND security_id IS NOT NULL
   AND security_id IN (SELECT security_id FROM universe_makeup
                        WHERE universe_id = @UNIVERSE_ID
                          AND universe_dt IN (SELECT DISTINCT universe_dt FROM #RANK_PARAMETERS)
                          AND security_id IS NOT NULL)

IF NOT EXISTS (SELECT * FROM #SECURITY_CLASS)
BEGIN
  INSERT #SECURITY_CLASS (security_id)
  SELECT DISTINCT security_id
    FROM universe_makeup
   WHERE universe_dt IN (SELECT DISTINCT universe_dt FROM #RANK_PARAMETERS)
     AND universe_id = @UNIVERSE_ID
END

UPDATE #SECURITY_CLASS
   SET country_cd = y.issue_country_cd
  FROM equity_common..security y
 WHERE #SECURITY_CLASS.security_id = y.security_id

IF @DEBUG = 1
BEGIN
  SELECT '#SECURITY_CLASS'
  SELECT * FROM #SECURITY_CLASS ORDER BY sector_id, segment_id, country_cd, security_id
END

CREATE TABLE #RANK_RESULTS (
  rank_event_id	int		NOT NULL,
  security_id	int		NULL,
  rank			int		NOT NULL,
  weight		float	NULL,
  weighted_rank	float	NULL
)

INSERT #RANK_RESULTS
SELECT o.rank_event_id, o.security_id, o.rank, p.weight, NULL
  FROM #RANK_PARAMETERS p, rank_output o
 WHERE p.rank_event_id = o.rank_event_id

IF @DEBUG = 1
BEGIN
  SELECT '#RANK_RESULTS: AFTER INITIAL INSERT'
  SELECT * FROM #RANK_RESULTS ORDER BY rank_event_id, rank, security_id
END

--OVERRIDE WEIGHT LOGIC: BEGIN
UPDATE #RANK_RESULTS
   SET weight = o.override_wgt
  FROM factor_against_weight_override o, #RANK_PARAMETERS p, #SECURITY_CLASS s
 WHERE #RANK_RESULTS.rank_event_id = p.rank_event_id
   AND #RANK_RESULTS.security_id = s.security_id
   AND o.factor_model_id = @FACTOR_MODEL_ID
   AND o.factor_id = p.factor_id
   AND o.against = p.against
   AND (o.against_id = p.against_id OR (o.against_id IS NULL AND p.against_id IS NULL))
   AND o.level_type = 'G'
   AND o.level_id = s.segment_id

UPDATE #RANK_RESULTS
   SET weight = o.override_wgt
  FROM factor_against_weight_override o, #RANK_PARAMETERS p, #SECURITY_CLASS s
 WHERE #RANK_RESULTS.rank_event_id = p.rank_event_id
   AND #RANK_RESULTS.security_id = s.security_id
   AND o.factor_model_id = @FACTOR_MODEL_ID
   AND o.factor_id = p.factor_id
   AND o.against = p.against
   AND (o.against_id = p.against_id OR (o.against_id IS NULL AND p.against_id IS NULL))
   AND o.level_type = 'C'
   AND o.level_id = s.sector_id

UPDATE #RANK_RESULTS
   SET weight = o.override_wgt
  FROM factor_against_weight_override o, #RANK_PARAMETERS p
 WHERE #RANK_RESULTS.rank_event_id = p.rank_event_id
   AND o.factor_model_id = @FACTOR_MODEL_ID
   AND o.factor_id = p.factor_id
   AND o.against = p.against
   AND (o.against_id = p.against_id OR (o.against_id IS NULL AND p.against_id IS NULL))
   AND o.level_type = 'U'
/*
NOTE: CURRENTLY NO CODE TO OVERRIDE COUNTRY WEIGHTS;
      REQUIRES ADDING COLUMN level_cd TO factor_against_weight_override */
--OVERRIDE WEIGHT LOGIC: END

IF @DEBUG = 1
BEGIN
  SELECT '#RANK_RESULTS: AFTER WEIGHT OVERRIDE UPDATE'
  SELECT * FROM #RANK_RESULTS ORDER BY rank_event_id, rank, security_id
END

UPDATE #RANK_RESULTS
   SET weighted_rank = CONVERT(float, rank) * ISNULL(weight, 0.0)

IF @DEBUG = 1
BEGIN
  SELECT '#RANK_RESULTS: AFTER WEIGHTED RANK UPDATE'
  SELECT * FROM #RANK_RESULTS ORDER BY rank_event_id, rank, security_id
END

CREATE TABLE #SCORES (
  security_id		int		NULL,
  sector_score		float	NULL,
  segment_score		float	NULL,
  ss_score			float	NULL,
  universe_score	float	NULL,
  country_score		float	NULL,
  total_score		float	NULL
)

INSERT #SCORES
SELECT r.security_id,
       SUM(CASE WHEN p.against = 'C' THEN r.weighted_rank ELSE 0.0 END),
       SUM(CASE WHEN p.against = 'G' THEN r.weighted_rank ELSE 0.0 END), NULL,
       SUM(CASE WHEN p.against = 'U' THEN r.weighted_rank ELSE 0.0 END),
       SUM(CASE WHEN p.against = 'Y' THEN r.weighted_rank ELSE 0.0 END), NULL
  FROM #RANK_RESULTS r, #RANK_PARAMETERS p
 WHERE r.rank_event_id = p.rank_event_id
 GROUP BY r.security_id

DROP TABLE #RANK_PARAMETERS
DROP TABLE #RANK_RESULTS

UPDATE #SCORES
   SET segment_score = (@GROUPS+1.0)/2.0
  FROM #SECURITY_CLASS s
 WHERE #SCORES.security_id = s.security_id
   AND (s.segment_id IS NULL OR #SCORES.segment_score = 0.0)

UPDATE #SCORES
   SET sector_score = (@GROUPS+1.0)/2.0
  FROM #SECURITY_CLASS s
 WHERE #SCORES.security_id = s.security_id
   AND (s.sector_id IS NULL OR #SCORES.sector_score = 0.0)

UPDATE #SCORES
   SET universe_score = (@GROUPS+1.0)/2.0
 WHERE universe_score IS NULL
    OR universe_score = 0.0

UPDATE #SCORES
   SET country_score = (@GROUPS+1.0)/2.0
 WHERE country_score IS NULL
    OR country_score = 0.0

IF @DEBUG = 1
BEGIN
  SELECT '#SCORES: AFTER INITIAL INSERT AND UPDATE'
  SELECT * FROM #SCORES ORDER BY security_id
END

IF EXISTS (SELECT * FROM decode WHERE item='REFRACTILE' AND code='SECTOR_SCORE' AND decode=@STRATEGY_ID)
  BEGIN EXEC scores_temp_rank_update @BDATE=@BDATE, @SCORE_TYPE='SECTOR_SCORE', @STRATEGY_ID=@STRATEGY_ID, @DEBUG=@DEBUG END
IF EXISTS (SELECT * FROM decode WHERE item='REFRACTILE' AND code='SEGMENT_SCORE' AND decode=@STRATEGY_ID)
  BEGIN EXEC scores_temp_rank_update @BDATE=@BDATE, @SCORE_TYPE='SEGMENT_SCORE', @STRATEGY_ID=@STRATEGY_ID, @DEBUG=@DEBUG END
IF EXISTS (SELECT * FROM decode WHERE item='REFRACTILE' AND code='UNIVERSE_SCORE' AND decode=@STRATEGY_ID)
  BEGIN EXEC scores_temp_rank_update @BDATE=@BDATE, @SCORE_TYPE='UNIVERSE_SCORE', @STRATEGY_ID=@STRATEGY_ID, @DEBUG=@DEBUG END
IF EXISTS (SELECT * FROM decode WHERE item='REFRACTILE' AND code='COUNTRY_SCORE' AND decode=@STRATEGY_ID)
  BEGIN EXEC scores_temp_rank_update @BDATE=@BDATE, @SCORE_TYPE='COUNTRY_SCORE', @STRATEGY_ID=@STRATEGY_ID, @DEBUG=@DEBUG END

IF @DEBUG = 1
BEGIN
  SELECT '#SCORES: AFTER RANKING AND UPDATE SCORES FOR SECTOR, SEGMENT, UNIVERSE, COUNTRY'
  SELECT * FROM #SCORES ORDER BY security_id
END

UPDATE #SCORES
   SET ss_score = sector_score * w.sector_ss_wgt + segment_score * w.segment_ss_wgt
  FROM #SECURITY_CLASS s, factor_model_weights w
 WHERE w.factor_model_id = @FACTOR_MODEL_ID
   AND #SCORES.security_id = s.security_id
   AND s.sector_id IS NULL
   AND s.segment_id IS NULL
   AND w.sector_id IS NULL
   AND w.segment_id IS NULL
   AND #SCORES.ss_score IS NULL

UPDATE #SCORES
   SET ss_score = sector_score * w.sector_ss_wgt + segment_score * w.segment_ss_wgt
  FROM #SECURITY_CLASS s, factor_model_weights w
 WHERE w.factor_model_id = @FACTOR_MODEL_ID
   AND #SCORES.security_id = s.security_id
   AND s.sector_id = w.sector_id
   AND s.segment_id = w.segment_id
   AND #SCORES.ss_score IS NULL

UPDATE #SCORES
   SET ss_score = sector_score * w.sector_ss_wgt + segment_score * w.segment_ss_wgt
  FROM #SECURITY_CLASS s, factor_model_weights w
 WHERE w.factor_model_id = @FACTOR_MODEL_ID
   AND #SCORES.security_id = s.security_id
   AND s.sector_id = w.sector_id
   AND #SCORES.ss_score IS NULL

IF @DEBUG = 1
BEGIN
  SELECT '#SCORES: AFTER CALCULATING SS_SCORE'
  SELECT * FROM #SCORES ORDER BY security_id
END

IF EXISTS (SELECT * FROM decode WHERE item='REFRACTILE' AND code='SS_SCORE' AND decode=@STRATEGY_ID)
  BEGIN EXEC scores_temp_rank_update @BDATE=@BDATE, @SCORE_TYPE='SS_SCORE', @STRATEGY_ID=@STRATEGY_ID, @DEBUG=@DEBUG END

IF @DEBUG = 1
BEGIN
  SELECT '#SCORES: AFTER RANKING AND UPDATING SS_SCORE'
  SELECT * FROM #SCORES ORDER BY security_id
END

UPDATE #SCORES
   SET total_score = ss_score * w.ss_total_wgt + universe_score * w.universe_total_wgt + country_score * w.country_total_wgt
  FROM #SECURITY_CLASS s, factor_model_weights w
 WHERE w.factor_model_id = @FACTOR_MODEL_ID
   AND #SCORES.security_id = s.security_id
   AND s.sector_id IS NULL
   AND s.segment_id IS NULL
   AND w.sector_id IS NULL
   AND w.segment_id IS NULL
   AND #SCORES.total_score IS NULL

UPDATE #SCORES
   SET total_score = ss_score * w.ss_total_wgt + universe_score * w.universe_total_wgt + country_score * w.country_total_wgt
  FROM #SECURITY_CLASS s, factor_model_weights w
 WHERE w.factor_model_id = @FACTOR_MODEL_ID
   AND #SCORES.security_id = s.security_id
   AND s.sector_id = w.sector_id
   AND s.segment_id = w.segment_id
   AND #SCORES.total_score IS NULL

UPDATE #SCORES
   SET total_score = ss_score * w.ss_total_wgt + universe_score * w.universe_total_wgt + country_score * w.country_total_wgt
  FROM #SECURITY_CLASS s, factor_model_weights w
 WHERE w.factor_model_id = @FACTOR_MODEL_ID
   AND #SCORES.security_id = s.security_id
   AND s.sector_id = w.sector_id
   AND #SCORES.total_score IS NULL

DROP TABLE #SECURITY_CLASS

IF @DEBUG = 1
BEGIN
  SELECT '#SCORES: AFTER CALCULATING TOTAL_SCORE'
  SELECT * FROM #SCORES ORDER BY security_id
END

IF EXISTS (SELECT * FROM decode WHERE item='REFRACTILE' AND code='TOTAL_SCORE' AND decode=@STRATEGY_ID)
  BEGIN EXEC scores_temp_rank_update @BDATE=@BDATE, @SCORE_TYPE='TOTAL_SCORE', @STRATEGY_ID=@STRATEGY_ID, @DEBUG=@DEBUG END

IF @DEBUG = 1
BEGIN
  SELECT '#SCORES: AFTER RANKING AND UPDATING TOTAL_SCORE'
  SELECT * FROM #SCORES
END

IF EXISTS (SELECT * FROM strategy WHERE strategy_id = @STRATEGY_ID AND rank_order = 0)
BEGIN
  UPDATE #SCORES
     SET sector_score = (@GROUPS+1.0) - sector_score,
         segment_score = (@GROUPS+1.0) - segment_score,
         ss_score = (@GROUPS+1.0) - ss_score,
         universe_score = (@GROUPS+1.0) - universe_score,
         country_score = (@GROUPS+1.0) - country_score,
         total_score = (@GROUPS+1.0) - total_score

  IF @DEBUG = 1
  BEGIN
    SELECT '#SCORES: AFTER REVERSING'
    SELECT * FROM #SCORES ORDER BY security_id
  END

  UPDATE rank_output
     SET rank = i.groups + 1 - rank
    FROM rank_inputs i
   WHERE i.bdate = @BDATE
     AND i.universe_id IN (SELECT universe_id FROM strategy WHERE strategy_id = @STRATEGY_ID)
     AND i.rank_event_id = rank_output.rank_event_id
END

DELETE scores
 WHERE bdate = @BDATE
   AND strategy_id = @STRATEGY_ID

INSERT scores
      (bdate, strategy_id, security_id,
       sector_score, segment_score, ss_score, universe_score, country_score, total_score)
SELECT @BDATE, @STRATEGY_ID, security_id,
       sector_score, segment_score, ss_score, universe_score, country_score, total_score
  FROM #SCORES

DROP TABLE #SCORES

RETURN 0
go
IF OBJECT_ID('dbo.strategy_scores_compute') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.strategy_scores_compute >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.strategy_scores_compute >>>'
go
