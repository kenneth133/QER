use QER
go
IF OBJECT_ID('dbo.universe_gng_populate') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.universe_gng_populate
    IF OBJECT_ID('dbo.universe_gng_populate') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.universe_gng_populate >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.universe_gng_populate >>>'
END
go
CREATE PROCEDURE dbo.universe_gng_populate @UNIVERSE_DT datetime = NULL,
                                           @UNIVERSE_ID int = NULL,
                                           @DEBUG bit = NULL
AS

IF @UNIVERSE_DT IS NULL
BEGIN
  SELECT 'ERROR: @UNIVERSE_DT IS A REQUIRED PARAMETER'
  RETURN -1
END

DECLARE @UNIVERSE_CD varchar(32),
        @RANK_EVENT_ID int,
        @FACTOR_ID int,
        @NOW datetime

SELECT @UNIVERSE_CD = universe_cd
  FROM universe_def
 WHERE universe_id = @UNIVERSE_ID

IF NOT EXISTS (SELECT * FROM universe_def WHERE universe_cd = @UNIVERSE_CD+'G') BEGIN RETURN 0 END
IF NOT EXISTS (SELECT * FROM universe_def WHERE universe_cd = @UNIVERSE_CD+'NG') BEGIN RETURN 0 END

CREATE TABLE #GNG_INPUT_FACTORS (
  security_id		int			NULL,
  price_book_value	float		NULL,
  price_book_rank	float		NULL,
  earn_trend_value	float		NULL,
  earn_trend_rank	float		NULL,
  earn_ltg_value	float		NULL,
  earn_ltg_rank		float		NULL,
  gng_factor_value	float		NULL,
  gng_factor_rank	float		NULL
)

INSERT #GNG_INPUT_FACTORS (security_id)
SELECT security_id
  FROM universe_makeup
 WHERE universe_dt = @UNIVERSE_DT
   AND universe_id = @UNIVERSE_ID

--EARNTREND_1TO4M: BEGIN
SELECT @FACTOR_ID = factor_id
  FROM factor
 WHERE factor_cd = 'EARNTREND_1TO4M'

EXEC rank_factor_universe @BDATE=@UNIVERSE_DT, @UNIVERSE_ID=@UNIVERSE_ID, @FACTOR_ID=@FACTOR_ID, @GROUPS=999, @AGAINST='U'

SELECT @RANK_EVENT_ID = MAX(rank_event_id)
  FROM rank_inputs
 WHERE bdate = @UNIVERSE_DT
   AND universe_id = @UNIVERSE_ID
   AND factor_id = @FACTOR_ID
   AND groups = 999
   AND against = 'U'

UPDATE #GNG_INPUT_FACTORS
   SET earn_trend_value = o.factor_value,
       earn_trend_rank = o.rank
  FROM rank_output o
 WHERE o.rank_event_id = @RANK_EVENT_ID
   AND #GNG_INPUT_FACTORS.security_id = o.security_id
--EARNTREND_1TO4M: END

--EARN_LTG: BEGIN
SELECT @FACTOR_ID = factor_id
  FROM factor
 WHERE factor_cd = 'EARN_LTG'

EXEC rank_factor_universe @BDATE=@UNIVERSE_DT, @UNIVERSE_ID=@UNIVERSE_ID, @FACTOR_ID=@FACTOR_ID, @GROUPS=999, @AGAINST='U'

SELECT @RANK_EVENT_ID = MAX(rank_event_id)
  FROM rank_inputs
 WHERE bdate = @UNIVERSE_DT
   AND universe_id = @UNIVERSE_ID
   AND factor_id = @FACTOR_ID
   AND groups = 999
   AND against = 'U'

UPDATE #GNG_INPUT_FACTORS
   SET earn_ltg_value = o.factor_value,
       earn_ltg_rank = o.rank
  FROM rank_output o
 WHERE o.rank_event_id = @RANK_EVENT_ID
   AND #GNG_INPUT_FACTORS.security_id = o.security_id
--EARN_LTG: END

--BKTOPRICE_GRC: BEGIN
UPDATE #GNG_INPUT_FACTORS
   SET price_book_value = CASE i.factor_value WHEN 0.0 THEN NULL ELSE i.factor_value END
  FROM instrument_factor i, factor f
 WHERE f.factor_cd = 'BKTOPRICE_GRC'
   AND i.factor_id = f.factor_id
   AND i.bdate = @UNIVERSE_DT
   AND i.security_id = #GNG_INPUT_FACTORS.security_id
   AND i.source_cd = 'FS'

UPDATE #GNG_INPUT_FACTORS
   SET price_book_value = 1.0/price_book_value
 WHERE price_book_value != 0.0

SELECT @FACTOR_ID = factor_id
  FROM factor
 WHERE factor_cd = 'DUMMY'

SELECT @NOW = GETDATE()

EXEC dummy_data_delete

INSERT instrument_factor
SELECT @UNIVERSE_DT, security_id, @FACTOR_ID, price_book_value, @NOW, 'FS'
  FROM #GNG_INPUT_FACTORS

EXEC rank_factor_universe @BDATE=@UNIVERSE_DT, @UNIVERSE_ID=@UNIVERSE_ID, @FACTOR_ID=@FACTOR_ID, @GROUPS=999, @AGAINST='U'

SELECT @RANK_EVENT_ID = MAX(rank_event_id)
  FROM rank_inputs
 WHERE bdate = @UNIVERSE_DT
   AND universe_id = @UNIVERSE_ID
   AND factor_id = @FACTOR_ID
   AND groups = 999
   AND against = 'U'

UPDATE #GNG_INPUT_FACTORS
   SET price_book_rank = o.rank
  FROM rank_output o
 WHERE o.rank_event_id = @RANK_EVENT_ID
   AND #GNG_INPUT_FACTORS.security_id = o.security_id
--BKTOPRICE_GRC: END

IF @DEBUG = 1
BEGIN
  SELECT '#GNG_INPUT_FACTORS'
  SELECT * FROM #GNG_INPUT_FACTORS ORDER BY security_id
END

--GNG_FACTOR: BEGIN
EXEC dummy_data_delete

INSERT instrument_factor
SELECT @UNIVERSE_DT, security_id, @FACTOR_ID,
      (2.0*((3.0*price_book_rank+earn_ltg_rank)/4.0)+earn_trend_rank)/3.0,
       @NOW, 'FS'
  FROM #GNG_INPUT_FACTORS

EXEC rank_factor_universe @BDATE=@UNIVERSE_DT, @UNIVERSE_ID=@UNIVERSE_ID, @FACTOR_ID=@FACTOR_ID, @GROUPS=999, @AGAINST='U'

SELECT @RANK_EVENT_ID = MAX(rank_event_id)
  FROM rank_inputs
 WHERE bdate = @UNIVERSE_DT
   AND universe_id = @UNIVERSE_ID
   AND factor_id = @FACTOR_ID
   AND groups = 999
   AND against = 'U'

UPDATE #GNG_INPUT_FACTORS
   SET gng_factor_value = o.factor_value,
       gng_factor_rank = o.rank
  FROM rank_output o
 WHERE o.rank_event_id = @RANK_EVENT_ID
   AND #GNG_INPUT_FACTORS.security_id = o.security_id
--GNG_FACTOR: END

IF @DEBUG = 1
BEGIN
  SELECT '#GNG_INPUT_FACTORS'
  SELECT * FROM #GNG_INPUT_FACTORS ORDER BY security_id
END

DROP TABLE #GNG_INPUT_FACTORS

DELETE universe_makeup
 WHERE universe_dt = @UNIVERSE_DT
   AND universe_id IN (SELECT universe_id FROM universe_def
                        WHERE universe_cd = @UNIVERSE_CD + 'G'
                           OR universe_cd = @UNIVERSE_CD + 'NG')

INSERT universe_makeup
SELECT @UNIVERSE_DT, d.universe_id, p.security_id, p.weight
  FROM universe_makeup p, universe_def d, rank_output o
 WHERE d.universe_cd = @UNIVERSE_CD + 'G'
   AND p.universe_id = @UNIVERSE_ID
   AND p.universe_dt = @UNIVERSE_DT
   AND p.security_id = o.security_id
   AND o.rank_event_id = @RANK_EVENT_ID
   AND o.rank >= 334

INSERT universe_makeup
SELECT @UNIVERSE_DT, d.universe_id, p.security_id, p.weight
  FROM universe_makeup p, universe_def d, rank_output o
 WHERE d.universe_cd = @UNIVERSE_CD + 'NG'
   AND p.universe_id = @UNIVERSE_ID
   AND p.universe_dt = @UNIVERSE_DT
   AND p.security_id = o.security_id
   AND o.rank_event_id = @RANK_EVENT_ID
   AND o.rank <= 333

EXEC dummy_data_delete

RETURN 0
go
IF OBJECT_ID('dbo.universe_gng_populate') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.universe_gng_populate >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.universe_gng_populate >>>'
go
