use QER
go
IF OBJECT_ID('dbo.model_portfolio_populate') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.model_portfolio_populate
    IF OBJECT_ID('dbo.model_portfolio_populate') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.model_portfolio_populate >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.model_portfolio_populate >>>'
END
go
CREATE PROCEDURE dbo.model_portfolio_populate @BDATE datetime = NULL,
                                              @STRATEGY_ID int = NULL,
                                              @DEBUG bit = NULL
AS

CREATE TABLE #MODEL_PORTFOLIO (
  security_id	int			NULL,
  ls_flag		bit			NULL, --1=LONG, 0=SHORT
  mkt_cap		float		NULL,
  eq_weight		float		NULL,
  cap_weight	float		NULL
)

DECLARE @MODEL_PORTFOLIO_DEF_CD varchar(32),
        @RANK_ORDER bit

SELECT @MODEL_PORTFOLIO_DEF_CD = d.model_portfolio_def_cd,
       @RANK_ORDER = g.rank_order
  FROM strategy g, model_portfolio_def d
 WHERE g.strategy_id = @STRATEGY_ID
   AND g.model_portfolio_def_id = d.model_portfolio_def_id

IF @MODEL_PORTFOLIO_DEF_CD = '80-100'
BEGIN--LONG 80-100
  INSERT #MODEL_PORTFOLIO (security_id, ls_flag)
  SELECT security_id, 1
    FROM scores
   WHERE bdate = @BDATE
     AND strategy_id = @STRATEGY_ID
     AND total_score >= 80.0
     AND total_score <= 100.0
END
ELSE IF @MODEL_PORTFOLIO_DEF_CD IN ('Q1', 'Q1-Q5', 'D1', 'D1-D10')
BEGIN
  DECLARE @UPPER_THRESHOLD int,
          @LOWER_THRESHOLD int,
          @DIVISOR int

  IF @MODEL_PORTFOLIO_DEF_CD LIKE '%Q%'
    BEGIN SELECT @DIVISOR = 5 END
  ELSE IF @MODEL_PORTFOLIO_DEF_CD LIKE '%D%'
    BEGIN SELECT @DIVISOR = 10 END

  SELECT @UPPER_THRESHOLD = fractile
    FROM strategy
   WHERE strategy_id = @STRATEGY_ID

  SELECT @LOWER_THRESHOLD = @UPPER_THRESHOLD / @DIVISOR
  SELECT @UPPER_THRESHOLD = @UPPER_THRESHOLD - @LOWER_THRESHOLD + 1

  IF @DEBUG = 1
  BEGIN
    SELECT '@DIVISOR', @DIVISOR
    SELECT '@LOWER_THRESHOLD', @LOWER_THRESHOLD
    SELECT '@UPPER_THRESHOLD', @UPPER_THRESHOLD
  END

  IF @RANK_ORDER = 1 --HIGHER IS BETTER
  BEGIN
    INSERT #MODEL_PORTFOLIO (security_id, ls_flag)
    SELECT security_id, 1
      FROM scores
     WHERE bdate = @BDATE
       AND strategy_id = @STRATEGY_ID
       AND total_score >= @UPPER_THRESHOLD

    IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
    BEGIN
      INSERT #MODEL_PORTFOLIO (security_id, ls_flag)
      SELECT security_id, 0
        FROM scores
       WHERE bdate = @BDATE
         AND strategy_id = @STRATEGY_ID
         AND total_score <= @LOWER_THRESHOLD
    END
  END
  ELSE IF @RANK_ORDER = 0 --LOWER IS BETTER
  BEGIN
    INSERT #MODEL_PORTFOLIO (security_id, ls_flag)
    SELECT security_id, 1
      FROM scores
     WHERE bdate = @BDATE
       AND strategy_id = @STRATEGY_ID
       AND total_score <= @LOWER_THRESHOLD

    IF @MODEL_PORTFOLIO_DEF_CD LIKE '%-%'
    BEGIN
      INSERT #MODEL_PORTFOLIO (security_id, ls_flag)
      SELECT security_id, 0
        FROM scores
       WHERE bdate = @BDATE
         AND strategy_id = @STRATEGY_ID
         AND total_score >= @UPPER_THRESHOLD
    END
  END
END

IF @DEBUG = 1
BEGIN
  SELECT '#MODEL_PORTFOLIO: AFTER INITIAL INSERT'
  SELECT * FROM #MODEL_PORTFOLIO
END

DECLARE @TOTAL_MCAP_LONG float,
        @TOTAL_MCAP_SHORT float,
        @COUNT_LONG float,
        @COUNT_SHORT float

UPDATE #MODEL_PORTFOLIO
   SET mkt_cap = market_cap_usd
  FROM equity_common..market_price p
 WHERE p.reference_date = @BDATE
   AND #MODEL_PORTFOLIO.security_id = p.security_id

SELECT @TOTAL_MCAP_LONG = SUM(mkt_cap) / 100.0
  FROM #MODEL_PORTFOLIO
 WHERE ls_flag = 1

SELECT @TOTAL_MCAP_SHORT = SUM(mkt_cap) / 100.0
  FROM #MODEL_PORTFOLIO
 WHERE ls_flag = 0

UPDATE #MODEL_PORTFOLIO
   SET cap_weight = mkt_cap / @TOTAL_MCAP_LONG
 WHERE ls_flag = 1

UPDATE #MODEL_PORTFOLIO
   SET cap_weight = -1.0 * mkt_cap / @TOTAL_MCAP_SHORT
 WHERE ls_flag = 0

IF @DEBUG = 1
BEGIN
  SELECT '#MODEL_PORTFOLIO: AFTER CALCULATING CAP_WEIGHT'
  SELECT * FROM #MODEL_PORTFOLIO
END

SELECT @COUNT_LONG = COUNT(*) / 100.0
  FROM #MODEL_PORTFOLIO
 WHERE ls_flag = 1

SELECT @COUNT_SHORT = COUNT(*) / 100.0
  FROM #MODEL_PORTFOLIO
 WHERE ls_flag = 0

IF @COUNT_LONG != 0.0
BEGIN
  UPDATE #MODEL_PORTFOLIO
     SET eq_weight = 1.0 / @COUNT_LONG
   WHERE ls_flag = 1
END

IF @COUNT_SHORT != 0.0
BEGIN
  UPDATE #MODEL_PORTFOLIO
     SET eq_weight = 1.0 / @COUNT_SHORT
   WHERE ls_flag = 0
END

IF @DEBUG = 1
BEGIN
  SELECT '#MODEL_PORTFOLIO: AFTER CALCULATING EQ_WEIGHT'
  SELECT * FROM #MODEL_PORTFOLIO
END

DECLARE @UNIVERSE_ID int

SELECT @UNIVERSE_ID = universe_id
  FROM universe_def
 WHERE universe_cd = (SELECT d.universe_cd + '_MPF_EQL'
                        FROM universe_def d, strategy s
                       WHERE s.strategy_id = @STRATEGY_ID
                         AND s.universe_id = d.universe_id)

DELETE universe_makeup
 WHERE universe_dt = @BDATE
   AND universe_id = @UNIVERSE_ID

INSERT universe_makeup
SELECT @BDATE, @UNIVERSE_ID, security_id, eq_weight
  FROM #MODEL_PORTFOLIO

SELECT @UNIVERSE_ID = universe_id
  FROM universe_def
 WHERE universe_cd = (SELECT d.universe_cd + '_MPF_CAP'
                        FROM universe_def d, strategy s
                       WHERE s.strategy_id = @STRATEGY_ID
                         AND s.universe_id = d.universe_id)

DELETE universe_makeup
 WHERE universe_dt = @BDATE
   AND universe_id = @UNIVERSE_ID

INSERT universe_makeup
SELECT @BDATE, @UNIVERSE_ID, security_id, cap_weight
  FROM #MODEL_PORTFOLIO

DROP TABLE #MODEL_PORTFOLIO

RETURN 0
go
IF OBJECT_ID('dbo.model_portfolio_populate') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.model_portfolio_populate >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.model_portfolio_populate >>>'
go
