use QER
go
IF OBJECT_ID('dbo.category_score_load') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.category_score_load
    IF OBJECT_ID('dbo.category_score_load') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.category_score_load >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.category_score_load >>>'
END
go
CREATE PROCEDURE dbo.category_score_load
@MODE varchar(16) = 'RELOAD',
@IDENTIFIER varchar(16) = 'CUSIP'
AS

SELECT @MODE = UPPER(@MODE)
SELECT @IDENTIFIER = UPPER(@IDENTIFIER)

IF @MODE NOT IN ('APPEND', 'OVERWRITE', 'RELOAD')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @MODE PARAMETER' RETURN -1 END
IF @MODE IN ('APPEND', 'OVERWRITE') AND @IDENTIFIER NOT IN ('TICKER', 'CUSIP', 'SEDOL', 'ISIN')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @IDENTIFIER PARAMETER' RETURN -1 END

UPDATE category_score_staging
   SET score_level = d.code
  FROM decode d
 WHERE d.item = 'SCORE_LEVEL'
   AND category_score_staging.score_level LIKE '%' + d.decode + '%'

UPDATE category_score_staging
   SET category_nm = d.decode
  FROM decode d
 WHERE d.item = 'FACTOR_CATEGORY'
   AND category_score_staging.category_nm = REPLACE(d.decode, ' ', '')

IF @MODE = 'RELOAD'
BEGIN
  DELETE category_score
    FROM (SELECT DISTINCT s.bdate, g.strategy_id
            FROM strategy g, category_score_staging s
           WHERE g.strategy_cd = s.strategy_cd) x
   WHERE category_score.bdate = x.bdate
     AND category_score.strategy_id = x.strategy_id
END
ELSE IF @MODE = 'OVERWRITE'
BEGIN
  IF @IDENTIFIER = 'TICKER'
  BEGIN
    DELETE category_score
      FROM strategy g, category_score_staging s, decode d
     WHERE s.strategy_cd = g.strategy_cd
       AND category_score.strategy_id = g.strategy_id
       AND category_score.bdate = s.bdate
       AND d.item = 'FACTOR_CATEGORY'
       AND category_score.category = d.code
       AND s.category_nm = d.decode
       AND category_score.score_level = s.score_level
       AND category_score.ticker = s.ticker
  END
  ELSE IF @IDENTIFIER = 'CUSIP'
  BEGIN
    DELETE category_score
      FROM strategy g, category_score_staging s, decode d
     WHERE s.strategy_cd = g.strategy_cd
       AND category_score.strategy_id = g.strategy_id
       AND category_score.bdate = s.bdate
       AND d.item = 'FACTOR_CATEGORY'
       AND category_score.category = d.code
       AND s.category_nm = d.decode
       AND category_score.score_level = s.score_level
       AND category_score.cusip = s.cusip
  END
  ELSE IF @IDENTIFIER = 'SEDOL'
  BEGIN
    DELETE category_score
      FROM strategy g, category_score_staging s, decode d
     WHERE s.strategy_cd = g.strategy_cd
       AND category_score.strategy_id = g.strategy_id
       AND category_score.bdate = s.bdate
       AND d.item = 'FACTOR_CATEGORY'
       AND category_score.category = d.code
       AND s.category_nm = d.decode
       AND category_score.score_level = s.score_level
       AND category_score.sedol = s.sedol
  END
  ELSE IF @IDENTIFIER = 'ISIN'
  BEGIN
    DELETE category_score
      FROM strategy g, category_score_staging s, decode d
     WHERE s.strategy_cd = g.strategy_cd
       AND category_score.strategy_id = g.strategy_id
       AND category_score.bdate = s.bdate
       AND d.item = 'FACTOR_CATEGORY'
       AND category_score.category = d.code
       AND s.category_nm = d.decode
       AND category_score.score_level = s.score_level
       AND category_score.isin = s.isin
  END
END
ELSE IF @MODE = 'APPEND'
BEGIN
  IF @IDENTIFIER = 'TICKER'
  BEGIN
    DELETE category_score_staging
      FROM strategy g, category_score c, decode d
     WHERE c.strategy_id = g.strategy_id
       AND category_score_staging.strategy_cd = g.strategy_cd
       AND category_score_staging.bdate = c.bdate
       AND d.item = 'FACTOR_CATEGORY'
       AND category_score_staging.category_nm = d.decode
       AND c.category = d.code
       AND category_score_staging.score_level = c.score_level
       AND category_score_staging.ticker = c.ticker
  END
  ELSE IF @IDENTIFIER = 'CUSIP'
  BEGIN
    DELETE category_score_staging
      FROM strategy g, category_score c, decode d
     WHERE c.strategy_id = g.strategy_id
       AND category_score_staging.strategy_cd = g.strategy_cd
       AND category_score_staging.bdate = c.bdate
       AND d.item = 'FACTOR_CATEGORY'
       AND category_score_staging.category_nm = d.decode
       AND c.category = d.code
       AND category_score_staging.score_level = c.score_level
       AND category_score_staging.cusip = c.cusip
  END
  ELSE IF @IDENTIFIER = 'SEDOL'
  BEGIN
    DELETE category_score_staging
      FROM strategy g, category_score c, decode d
     WHERE c.strategy_id = g.strategy_id
       AND category_score_staging.strategy_cd = g.strategy_cd
       AND category_score_staging.bdate = c.bdate
       AND d.item = 'FACTOR_CATEGORY'
       AND category_score_staging.category_nm = d.decode
       AND c.category = d.code
       AND category_score_staging.score_level = c.score_level
       AND category_score_staging.sedol = c.sedol
  END
  ELSE IF @IDENTIFIER = 'ISIN'
  BEGIN
    DELETE category_score_staging
      FROM strategy g, category_score c, decode d
     WHERE c.strategy_id = g.strategy_id
       AND category_score_staging.strategy_cd = g.strategy_cd
       AND category_score_staging.bdate = c.bdate
       AND d.item = 'FACTOR_CATEGORY'
       AND category_score_staging.category_nm = d.decode
       AND c.category = d.code
       AND category_score_staging.score_level = c.score_level
       AND category_score_staging.isin = c.isin
  END
END

INSERT category_score
      (bdate, strategy_id, mqa_id, ticker, cusip, sedol, isin, gv_key, score_level, category, category_score)
SELECT s.bdate, g.strategy_id, s.mqa_id, s.ticker, s.cusip, s.sedol, s.isin, s.gv_key, s.score_level, d.code, s.category_score
  FROM strategy g, category_score_staging s, decode d
 WHERE g.strategy_cd = s.strategy_cd
   AND d.item = 'FACTOR_CATEGORY'
   AND d.decode = s.category_nm

RETURN 0
go
IF OBJECT_ID('dbo.category_score_load') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.category_score_load >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.category_score_load >>>'
go
