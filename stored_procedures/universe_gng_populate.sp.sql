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
        @FACTOR_ID int

SELECT @UNIVERSE_CD = universe_cd
  FROM universe_def
 WHERE universe_id = @UNIVERSE_ID

IF NOT EXISTS (SELECT * FROM universe_def WHERE universe_cd = @UNIVERSE_CD+'G') BEGIN RETURN 0 END
IF NOT EXISTS (SELECT * FROM universe_def WHERE universe_cd = @UNIVERSE_CD+'NG') BEGIN RETURN 0 END

CREATE TABLE #GNG_INPUT_FACTORS (
  mqa_id		varchar(32)	NULL,
  ticker		varchar(16)	NULL,
  cusip			varchar(32)	NULL,
  sedol			varchar(32)	NULL,
  isin			varchar(64)	NULL,
  gv_key		int		NULL,
  price_book_value	float		NULL,
  price_book_rank	float		NULL,
  earn_trend_value	float		NULL,
  earn_trend_rank	float		NULL,
  earn_ltg_value	float		NULL,
  earn_ltg_rank		float		NULL,
  gng_factor_value	float		NULL,
  gng_factor_rank	float		NULL
)

INSERT #GNG_INPUT_FACTORS (mqa_id, ticker, cusip, sedol, isin, gv_key)
SELECT mqa_id, ticker, cusip, sedol, isin, gv_key
  FROM universe_makeup
 WHERE universe_dt = @UNIVERSE_DT
   AND universe_id = @UNIVERSE_ID

BEGIN--EARNTREND_1TO4M
  SELECT @FACTOR_ID = factor_id
    FROM factor
   WHERE factor_cd = 'EARNTREND_1TO4M'

  SELECT @RANK_EVENT_ID = MAX(rank_event_id) + 1
    FROM rank_inputs

  EXEC rank_factor_universe @BDATE=@UNIVERSE_DT, @UNIVERSE_ID=@UNIVERSE_ID, @FACTOR_ID=@FACTOR_ID, @GROUPS=999, @AGAINST='U'

  UPDATE #GNG_INPUT_FACTORS
     SET earn_trend_value = o.factor_value,
         earn_trend_rank = o.rank
    FROM rank_output o
   WHERE o.rank_event_id = @RANK_EVENT_ID
     AND #GNG_INPUT_FACTORS.cusip = o.cusip
END--EARNTREND_1TO4M

BEGIN--EARN_LTG
  SELECT @FACTOR_ID = factor_id
    FROM factor
   WHERE factor_cd = 'EARN_LTG'

  SELECT @RANK_EVENT_ID = MAX(rank_event_id) + 1
    FROM rank_inputs

  EXEC rank_factor_universe @BDATE=@UNIVERSE_DT, @UNIVERSE_ID=@UNIVERSE_ID, @FACTOR_ID=@FACTOR_ID, @GROUPS=999, @AGAINST='U'

  UPDATE #GNG_INPUT_FACTORS
     SET earn_ltg_value = o.factor_value,
         earn_ltg_rank = o.rank
    FROM rank_output o
   WHERE o.rank_event_id = @RANK_EVENT_ID
     AND #GNG_INPUT_FACTORS.cusip = o.cusip
END--EARN_LTG

BEGIN--BKTOPRICE_GRC
  UPDATE #GNG_INPUT_FACTORS
     SET price_book_value = CASE i.factor_value WHEN 0.0 THEN NULL ELSE i.factor_value END
    FROM instrument_factor i, factor f
   WHERE f.factor_cd = 'BKTOPRICE_GRC'
     AND i.factor_id = f.factor_id
     AND i.bdate = @UNIVERSE_DT
     AND i.cusip = #GNG_INPUT_FACTORS.cusip
     AND i.source_cd = 'FS'

  UPDATE #GNG_INPUT_FACTORS
     SET price_book_value = 1.0/price_book_value
   WHERE price_book_value != 0.0

  SELECT @FACTOR_ID = factor_id
    FROM QER..factor
   WHERE factor_cd = 'DUMMY'

  DELETE instrument_factor_staging

  INSERT instrument_factor_staging
  SELECT @UNIVERSE_DT, mqa_id, ticker, cusip, sedol, isin, gv_key,
         'DUMMY', price_book_value
    FROM #GNG_INPUT_FACTORS

  EXEC instrument_factor_load @SOURCE_CD='FS'

  SELECT @RANK_EVENT_ID = MAX(rank_event_id) + 1
    FROM rank_inputs

  EXEC rank_factor_universe @BDATE=@UNIVERSE_DT, @UNIVERSE_ID=@UNIVERSE_ID, @FACTOR_ID=@FACTOR_ID, @GROUPS=999, @AGAINST='U'

  UPDATE #GNG_INPUT_FACTORS
     SET price_book_rank = o.rank
    FROM rank_output o
   WHERE o.rank_event_id = @RANK_EVENT_ID
     AND #GNG_INPUT_FACTORS.cusip = o.cusip

  EXEC dummy_data_delete
END--BKTOPRICE_GRC

IF @DEBUG = 1
BEGIN
  SELECT '#GNG_INPUT_FACTORS'
  SELECT * FROM #GNG_INPUT_FACTORS ORDER BY ticker, cusip
END

BEGIN--GNG_FACTOR
  DELETE instrument_factor_staging

  INSERT instrument_factor_staging
  SELECT @UNIVERSE_DT, mqa_id, ticker, cusip, sedol, isin, gv_key,
         'DUMMY', (2.0*((3.0*price_book_rank+earn_ltg_rank)/4.0)+earn_trend_rank)/3.0
    FROM #GNG_INPUT_FACTORS

  EXEC instrument_factor_load @SOURCE_CD='FS'

  SELECT @RANK_EVENT_ID = MAX(rank_event_id) + 1
    FROM rank_inputs

  EXEC rank_factor_universe @BDATE=@UNIVERSE_DT, @UNIVERSE_ID=@UNIVERSE_ID, @FACTOR_ID=@FACTOR_ID, @GROUPS=999, @AGAINST='U'

  UPDATE #GNG_INPUT_FACTORS
     SET gng_factor_value = o.factor_value,
         gng_factor_rank = o.rank
    FROM rank_output o
   WHERE o.rank_event_id = @RANK_EVENT_ID
     AND #GNG_INPUT_FACTORS.cusip = o.cusip
END--GNG_FACTOR

IF @DEBUG = 1
BEGIN
  SELECT '#GNG_INPUT_FACTORS'
  SELECT * FROM #GNG_INPUT_FACTORS ORDER BY ticker, cusip
END

DROP TABLE #GNG_INPUT_FACTORS

DELETE universe_makeup
 WHERE universe_dt = @UNIVERSE_DT
   AND universe_id IN (SELECT universe_id FROM universe_def
                        WHERE universe_cd = @UNIVERSE_CD + 'G'
                           OR universe_cd = @UNIVERSE_CD + 'NG')

INSERT universe_makeup
SELECT @UNIVERSE_DT, d.universe_id,
       p.mqa_id, p.ticker, p.cusip, p.sedol, p.isin, p.gv_key, p.weight
  FROM universe_makeup p, universe_def d, rank_output o
 WHERE d.universe_cd = @UNIVERSE_CD + 'G'
   AND p.universe_id = @UNIVERSE_ID
   AND p.universe_dt = @UNIVERSE_DT
   AND p.cusip = o.cusip
   AND o.rank_event_id = @RANK_EVENT_ID
   AND o.rank >= 334

INSERT universe_makeup
SELECT @UNIVERSE_DT, d.universe_id,
       p.mqa_id, p.ticker, p.cusip, p.sedol, p.isin, p.gv_key, p.weight
  FROM universe_makeup p, universe_def d, rank_output o
 WHERE d.universe_cd = @UNIVERSE_CD + 'NG'
   AND p.universe_id = @UNIVERSE_ID
   AND p.universe_dt = @UNIVERSE_DT
   AND p.cusip = o.cusip
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