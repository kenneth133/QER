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
CREATE PROCEDURE dbo.strategy_scores_compute @BDATE datetime = NULL, --optional, defaults to previous business day
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
        @GROUPS			int

SELECT @FACTOR_MODEL_ID = s.factor_model_id,
       @SECTOR_MODEL_ID = f.sector_model_id,
       @UNIVERSE_ID = s.universe_id,
       @GROUPS = s.fractile
  FROM QER..strategy s, QER..factor_model f
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
  rank_event_id		int		NULL,
  as_of_date		datetime	NOT NULL,
  bdate			datetime	NOT NULL,
  universe_dt		datetime	NOT NULL,
  universe_id		int		NOT NULL,
  factor_id		int		NOT NULL,
  groups		int		NOT NULL,
  against		varchar(1)	NOT NULL,
  against_id		int		NULL,
  weight		float		NULL
)

INSERT #RANK_PARAMETERS (as_of_date, bdate, universe_dt, universe_id, factor_id, groups, against, against_id, weight)
SELECT max(r.as_of_date), r.bdate, r.universe_dt, r.universe_id, r.factor_id, r.groups, r.against, r.against_id, f.weight
  FROM QER..factor_against_weight f, QER..rank_inputs r
 WHERE f.factor_model_id = @FACTOR_MODEL_ID
   AND f.factor_id = r.factor_id
   AND f.against = r.against
   AND f.against_id = r.against_id
   AND r.bdate = @BDATE
   AND r.universe_id = @UNIVERSE_ID
   AND r.groups = @GROUPS
 GROUP BY r.bdate, r.universe_dt, r.universe_id, r.factor_id, r.groups, r.against, r.against_id, f.weight

INSERT #RANK_PARAMETERS (as_of_date, bdate, universe_dt, universe_id, factor_id, groups, against, weight)
SELECT max(r.as_of_date), r.bdate, r.universe_dt, r.universe_id, r.factor_id, r.groups, r.against, f.weight
  FROM QER..factor_against_weight f, QER..rank_inputs r
 WHERE f.factor_model_id = @FACTOR_MODEL_ID
   AND f.factor_id = r.factor_id
   AND f.against = r.against
   AND f.against_id IS NULL
   AND r.against_id IS NULL
   AND r.bdate = @BDATE
   AND r.universe_id = @UNIVERSE_ID
   AND r.groups = @GROUPS
 GROUP BY r.bdate, r.universe_dt, r.universe_id, r.factor_id, r.groups, r.against, f.weight

UPDATE #RANK_PARAMETERS
   SET rank_event_id = i.rank_event_id
  FROM rank_inputs i
 WHERE #RANK_PARAMETERS.as_of_date = i.as_of_date
   AND #RANK_PARAMETERS.bdate = i.bdate
   AND #RANK_PARAMETERS.universe_dt = i.universe_dt
   AND #RANK_PARAMETERS.universe_id = i.universe_id
   AND #RANK_PARAMETERS.factor_id = i.factor_id
   AND #RANK_PARAMETERS.groups = i.groups
   AND #RANK_PARAMETERS.against = i.against
   AND #RANK_PARAMETERS.against_id = i.against_id

UPDATE #RANK_PARAMETERS
   SET rank_event_id = i.rank_event_id
  FROM rank_inputs i
 WHERE #RANK_PARAMETERS.as_of_date = i.as_of_date
   AND #RANK_PARAMETERS.bdate = i.bdate
   AND #RANK_PARAMETERS.universe_dt = i.universe_dt
   AND #RANK_PARAMETERS.universe_id = i.universe_id
   AND #RANK_PARAMETERS.factor_id = i.factor_id
   AND #RANK_PARAMETERS.groups = i.groups
   AND #RANK_PARAMETERS.against = i.against
   AND #RANK_PARAMETERS.against_id IS NULL
   AND i.against_id IS NULL

IF @DEBUG = 1
BEGIN
  SELECT '#RANK_PARAMETERS'
  SELECT * FROM #RANK_PARAMETERS ORDER BY rank_event_id
END

CREATE TABLE #SS_SECURITY (
  sector_id	int		NULL,
  segment_id	int		NULL,
  mqa_id	varchar(32)	NULL,
  ticker	varchar(16)	NULL,
  cusip		varchar(32)	NULL,
  sedol		varchar(32)	NULL,
  isin		varchar(64)	NULL,
  gv_key	int		NULL
)

INSERT #SS_SECURITY
SELECT sector_id, segment_id, mqa_id, ticker, cusip, sedol, isin, gv_key
  FROM QER..sector_model_security
 WHERE bdate = @BDATE
   AND sector_model_id = @SECTOR_MODEL_ID
   AND cusip IS NOT NULL
   AND cusip IN (SELECT cusip FROM universe_makeup
                  WHERE universe_id = @UNIVERSE_ID
                    AND universe_dt IN (SELECT DISTINCT universe_dt FROM #RANK_PARAMETERS)
                    AND cusip IS NOT NULL)

IF NOT EXISTS (SELECT * FROM #SS_SECURITY)
BEGIN
  INSERT #SS_SECURITY
  SELECT DISTINCT NULL, NULL, mqa_id, ticker, cusip, sedol, isin, gv_key
    FROM QER..universe_makeup
   WHERE universe_dt IN (SELECT DISTINCT universe_dt FROM #RANK_PARAMETERS)
     AND universe_id = @UNIVERSE_ID
END

IF @DEBUG = 1
BEGIN
  SELECT '#SS_SECURITY'
  SELECT * FROM #SS_SECURITY ORDER BY sector_id, segment_id, cusip, isin
END

CREATE TABLE #RANK_RESULTS (
  rank_event_id	int		NOT NULL,
  mqa_id	varchar(32)	NULL,
  ticker	varchar(16)	NULL,
  cusip		varchar(32)	NULL,
  sedol		varchar(32)	NULL,
  isin		varchar(64)	NULL,
  gv_key	int		NULL,
  rank		int		NOT NULL,
  weight	float		NULL,
  weighted_rank	float		NULL
)

INSERT #RANK_RESULTS
SELECT o.rank_event_id, o.mqa_id, o.ticker, o.cusip, o.sedol, o.isin, o.gv_key, o.rank, p.weight, NULL
  FROM #RANK_PARAMETERS p, QER..rank_output o
 WHERE p.rank_event_id = o.rank_event_id

IF @DEBUG = 1
BEGIN
  SELECT '#RANK_RESULTS: AFTER INITIAL INSERT'
  SELECT * FROM #RANK_RESULTS ORDER BY rank_event_id, rank, cusip, isin
END

--OVERRIDE WEIGHT LOGIC: BEGIN
UPDATE #RANK_RESULTS
   SET weight = o.override_wgt
  FROM QER..factor_against_weight_override o, #RANK_PARAMETERS p, #SS_SECURITY s
 WHERE #RANK_RESULTS.rank_event_id = p.rank_event_id
   AND #RANK_RESULTS.cusip = s.cusip
   AND o.factor_model_id = @FACTOR_MODEL_ID
   AND o.factor_id = p.factor_id
   AND o.against = p.against
   AND (o.against_id = p.against_id OR (o.against_id IS NULL AND p.against_id IS NULL))
   AND o.level_type = 'G'
   AND o.level_id = s.segment_id

UPDATE #RANK_RESULTS
   SET weight = o.override_wgt
  FROM QER..factor_against_weight_override o, #RANK_PARAMETERS p, #SS_SECURITY s
 WHERE #RANK_RESULTS.rank_event_id = p.rank_event_id
   AND #RANK_RESULTS.cusip = s.cusip
   AND o.factor_model_id = @FACTOR_MODEL_ID
   AND o.factor_id = p.factor_id
   AND o.against = p.against
   AND (o.against_id = p.against_id OR (o.against_id IS NULL AND p.against_id IS NULL))
   AND o.level_type = 'C'
   AND o.level_id = s.sector_id

UPDATE #RANK_RESULTS
   SET weight = o.override_wgt
  FROM QER..factor_against_weight_override o, #RANK_PARAMETERS p
 WHERE #RANK_RESULTS.rank_event_id = p.rank_event_id
   AND o.factor_model_id = @FACTOR_MODEL_ID
   AND o.factor_id = p.factor_id
   AND o.against = p.against
   AND (o.against_id = p.against_id OR (o.against_id IS NULL AND p.against_id IS NULL))
   AND o.level_type = 'U'
--OVERRIDE WEIGHT LOGIC: END

IF @DEBUG = 1
BEGIN
  SELECT '#RANK_RESULTS: AFTER WEIGHT OVERRIDE UPDATE'
  SELECT * FROM #RANK_RESULTS ORDER BY rank_event_id, rank, cusip, isin
END

UPDATE #RANK_RESULTS
   SET weighted_rank = rank * ISNULL(weight, 0.0)

IF @DEBUG = 1
BEGIN
  SELECT '#RANK_RESULTS: AFTER WEIGHTED RANK UPDATE'
  SELECT * FROM #RANK_RESULTS ORDER BY rank_event_id, rank, cusip, isin
END

CREATE TABLE #SCORES (
  mqa_id		varchar(32)	NULL,
  ticker		varchar(16)	NULL,
  cusip			varchar(32)	NULL,
  sedol			varchar(32)	NULL,
  isin			varchar(64)	NULL,
  gv_key		int		NULL,
  sector_score		float		NULL,
  segment_score		float		NULL,
  ss_score		float		NULL,
  universe_score	float		NULL,
  total_score		float		NULL
)

INSERT #SCORES
SELECT r.mqa_id, r.ticker, r.cusip, r.sedol, r.isin, r.gv_key,
       SUM(CASE WHEN p.against = 'C' THEN r.weighted_rank ELSE 0.0 END),
       SUM(CASE WHEN p.against = 'G' THEN r.weighted_rank ELSE 0.0 END), 0.0,
       SUM(CASE WHEN p.against = 'U' THEN r.weighted_rank ELSE 0.0 END), 0.0
  FROM #RANK_RESULTS r, #RANK_PARAMETERS p
 WHERE r.rank_event_id = p.rank_event_id
 GROUP BY r.mqa_id, r.ticker, r.cusip, r.sedol, r.isin, r.gv_key

UPDATE #SCORES
   SET segment_score = (@GROUPS+1.0)/2.0
  FROM #SS_SECURITY ss
 WHERE #SCORES.cusip = ss.cusip
   AND ss.segment_id IS NULL

UPDATE #SCORES
   SET sector_score = (@GROUPS+1.0)/2.0
  FROM #SS_SECURITY ss
 WHERE #SCORES.cusip = ss.cusip
   AND ss.sector_id IS NULL

UPDATE #SCORES
   SET universe_score = (@GROUPS+1.0)/2.0
 WHERE universe_score IS NULL
    OR universe_score = 0.0

IF @DEBUG = 1
BEGIN
  SELECT '#SCORES: AFTER INITIAL INSERT AND UPDATE'
  SELECT * FROM #SCORES
END

IF NOT EXISTS (SELECT * FROM decode WHERE item='NO REFRACTILE' AND code='SECTOR_SCORE' AND decode=@STRATEGY_ID)
  BEGIN EXEC scores_temp_rank_update @BDATE=@BDATE, @SCORE_TYPE='SECTOR_SCORE', @STRATEGY_ID=@STRATEGY_ID END
IF NOT EXISTS (SELECT * FROM decode WHERE item='NO REFRACTILE' AND code='SEGMENT_SCORE' AND decode=@STRATEGY_ID)
  BEGIN EXEC scores_temp_rank_update @BDATE=@BDATE, @SCORE_TYPE='SEGMENT_SCORE', @STRATEGY_ID=@STRATEGY_ID END
IF NOT EXISTS (SELECT * FROM decode WHERE item='NO REFRACTILE' AND code='UNIVERSE_SCORE' AND decode=@STRATEGY_ID)
  BEGIN EXEC scores_temp_rank_update @BDATE=@BDATE, @SCORE_TYPE='UNIVERSE_SCORE', @STRATEGY_ID=@STRATEGY_ID END

IF @DEBUG = 1
BEGIN
  SELECT '#SCORES: AFTER RANKING AND UPDATE SCORES FOR SECTOR, SEGMENT, UNIVERSE'
  SELECT * FROM #SCORES ORDER BY cusip
END

UPDATE #SCORES
   SET ss_score = sector_score * w.sector_ss_wgt + segment_score * segment_ss_wgt
  FROM #SS_SECURITY s, QER..factor_model_weights w
 WHERE w.factor_model_id = @FACTOR_MODEL_ID
   AND #SCORES.cusip = s.cusip
   AND s.sector_id IS NULL
   AND s.segment_id IS NULL
   AND w.sector_id IS NULL
   AND w.segment_id IS NULL

UPDATE #SCORES
   SET ss_score = sector_score * w.sector_ss_wgt + segment_score * segment_ss_wgt
  FROM #SS_SECURITY s, QER..factor_model_weights w
 WHERE w.factor_model_id = @FACTOR_MODEL_ID
   AND #SCORES.cusip = s.cusip
   AND s.sector_id = w.sector_id
   AND s.segment_id IS NULL
   AND w.segment_id IS NULL

UPDATE #SCORES
   SET ss_score = sector_score * w.sector_ss_wgt + segment_score * segment_ss_wgt
  FROM #SS_SECURITY s, QER..factor_model_weights w
 WHERE w.factor_model_id = @FACTOR_MODEL_ID
   AND #SCORES.cusip = s.cusip
   AND s.sector_id = w.sector_id
   AND s.segment_id = w.segment_id

IF @DEBUG = 1
BEGIN
  SELECT '#SCORES: AFTER CALCULATING SS_SCORE'
  SELECT * FROM #SCORES ORDER BY cusip
END

IF NOT EXISTS (SELECT * FROM decode WHERE item='NO REFRACTILE' AND code='SS_SCORE' AND decode=@STRATEGY_ID)
  BEGIN EXEC scores_temp_rank_update @BDATE=@BDATE, @SCORE_TYPE='SS_SCORE', @STRATEGY_ID=@STRATEGY_ID END

IF @DEBUG = 1
BEGIN
  SELECT '#SCORES: AFTER RANKING AND UPDATING SS_SCORE'
  SELECT * FROM #SCORES ORDER BY cusip
END

UPDATE #SCORES
   SET total_score = ss_score * w.ss_total_wgt + universe_score * w.universe_total_wgt
  FROM #SS_SECURITY s, QER..factor_model_weights w
 WHERE w.factor_model_id = @FACTOR_MODEL_ID
   AND #SCORES.cusip = s.cusip
   AND s.sector_id IS NULL
   AND s.segment_id IS NULL
   AND w.sector_id IS NULL
   AND w.segment_id IS NULL

UPDATE #SCORES
   SET total_score = ss_score * w.ss_total_wgt + universe_score * w.universe_total_wgt
  FROM #SS_SECURITY s, QER..factor_model_weights w
 WHERE w.factor_model_id = @FACTOR_MODEL_ID
   AND #SCORES.cusip = s.cusip
   AND s.sector_id = w.sector_id
   AND s.segment_id IS NULL
   AND w.segment_id IS NULL

UPDATE #SCORES
   SET total_score = ss_score * w.ss_total_wgt + universe_score * w.universe_total_wgt
  FROM #SS_SECURITY s, QER..factor_model_weights w
 WHERE w.factor_model_id = @FACTOR_MODEL_ID
   AND #SCORES.cusip = s.cusip
   AND s.sector_id = w.sector_id
   AND s.segment_id = w.segment_id

IF @DEBUG = 1
BEGIN
  SELECT '#SCORES: AFTER CALCULATING TOTAL_SCORE'
  SELECT * FROM #SCORES ORDER BY cusip
END

IF NOT EXISTS (SELECT * FROM decode WHERE item='NO REFRACTILE' AND code='TOTAL_SCORE' AND decode=@STRATEGY_ID)
  BEGIN EXEC scores_temp_rank_update @BDATE=@BDATE, @SCORE_TYPE='TOTAL_SCORE', @STRATEGY_ID=@STRATEGY_ID END

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
         total_score = (@GROUPS+1.0) - total_score

  IF @DEBUG = 1
  BEGIN
    SELECT '#SCORES: AFTER REVERSING'
    SELECT * FROM #SCORES ORDER BY cusip
  END

  UPDATE rank_output
     SET rank = i.groups + 1 - rank
    FROM rank_inputs i
   WHERE i.bdate = @BDATE
     AND i.universe_id IN (SELECT universe_id FROM strategy WHERE strategy_id = @STRATEGY_ID)
     AND i.rank_event_id = rank_output.rank_event_id
END

DELETE QER..scores
 WHERE bdate = @BDATE
   AND strategy_id = @STRATEGY_ID

INSERT QER..scores
      (bdate, strategy_id, mqa_id, ticker, cusip, sedol, isin, gv_key,
       sector_score, segment_score, ss_score, universe_score, total_score)
SELECT @BDATE, @STRATEGY_ID, mqa_id, ticker, cusip, sedol, isin, gv_key,
       sector_score, segment_score, ss_score, universe_score, total_score
  FROM #SCORES

RETURN 0
go
IF OBJECT_ID('dbo.strategy_scores_compute') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.strategy_scores_compute >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.strategy_scores_compute >>>'
go