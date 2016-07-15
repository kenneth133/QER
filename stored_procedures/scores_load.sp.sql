use QER
go
IF OBJECT_ID('dbo.scores_load') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.scores_load
    IF OBJECT_ID('dbo.scores_load') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.scores_load >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.scores_load >>>'
END
go
CREATE PROCEDURE dbo.scores_load @MODE varchar(16) = 'RELOAD',
                                 @IDENTIFIER varchar(16) = 'CUSIP'
AS

SELECT @MODE = UPPER(@MODE)
SELECT @IDENTIFIER = UPPER(@IDENTIFIER)

IF @MODE NOT IN ('APPEND', 'OVERWRITE', 'RELOAD')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @MODE PARAMETER' RETURN -1 END
IF @MODE IN ('APPEND', 'OVERWRITE') AND @IDENTIFIER NOT IN ('TICKER', 'CUSIP', 'SEDOL', 'ISIN')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @IDENTIFIER PARAMETER' RETURN -1 END

IF @MODE = 'RELOAD'
BEGIN
  DELETE scores
    FROM strategy g, scores_staging s
   WHERE s.strategy_cd = g.strategy_cd
     AND scores.strategy_id = g.strategy_id
     AND scores.bdate = s.bdate
END
ELSE IF @MODE = 'OVERWRITE'
BEGIN
  IF @IDENTIFIER = 'TICKER'
  BEGIN
    DELETE scores
      FROM strategy g, scores_staging s
     WHERE s.strategy_cd = g.strategy_cd
       AND scores.strategy_id = g.strategy_id
       AND scores.bdate = s.bdate
       AND scores.ticker = s.ticker
  END
  ELSE IF @IDENTIFIER = 'CUSIP'
  BEGIN
    DELETE scores
      FROM strategy g, scores_staging s
     WHERE s.strategy_cd = g.strategy_cd
       AND scores.strategy_id = g.strategy_id
       AND scores.bdate = s.bdate
       AND scores.cusip = s.cusip
  END
  ELSE IF @IDENTIFIER = 'SEDOL'
  BEGIN
    DELETE scores
      FROM strategy g, scores_staging s
     WHERE s.strategy_cd = g.strategy_cd
       AND scores.strategy_id = g.strategy_id
       AND scores.bdate = s.bdate
       AND scores.sedol = s.sedol
  END
  ELSE IF @IDENTIFIER = 'ISIN'
  BEGIN
    DELETE scores
      FROM strategy g, scores_staging s
     WHERE s.strategy_cd = g.strategy_cd
       AND scores.strategy_id = g.strategy_id
       AND scores.bdate = s.bdate
       AND scores.isin = s.isin
  END
END
ELSE IF @MODE = 'APPEND'
BEGIN
  IF @IDENTIFIER = 'TICKER'
  BEGIN
    DELETE scores_staging
      FROM strategy g, scores s
     WHERE s.strategy_id = g.strategy_id
       AND scores_staging.strategy_cd = g.strategy_cd
       AND scores_staging.bdate = s.bdate
       AND scores_staging.ticker = s.ticker
  END
  ELSE IF @IDENTIFIER = 'CUSIP'
  BEGIN
    DELETE scores_staging
      FROM strategy g, scores s
     WHERE s.strategy_id = g.strategy_id
       AND scores_staging.strategy_cd = g.strategy_cd
       AND scores_staging.bdate = s.bdate
       AND scores_staging.cusip = s.cusip
  END
  ELSE IF @IDENTIFIER = 'SEDOL'
  BEGIN
    DELETE scores_staging
      FROM strategy g, scores s
     WHERE s.strategy_id = g.strategy_id
       AND scores_staging.strategy_cd = g.strategy_cd
       AND scores_staging.bdate = s.bdate
       AND scores_staging.sedol = s.sedol
  END
  ELSE IF @IDENTIFIER = 'ISIN'
  BEGIN
    DELETE scores_staging
      FROM strategy g, scores s
     WHERE s.strategy_id = g.strategy_id
       AND scores_staging.strategy_cd = g.strategy_cd
       AND scores_staging.bdate = s.bdate
       AND scores_staging.isin = s.isin
  END
END

INSERT scores
      (bdate, strategy_id, mqa_id, ticker, cusip, sedol, isin, gv_key,
       sector_score, segment_score, ss_score, universe_score, country_score, total_score)
SELECT s.bdate, g.strategy_id, s.mqa_id, s.ticker, s.cusip, s.sedol, s.isin, s.gv_key,
       s.sector_score, s.segment_score, s.ss_score, s.universe_score, s.country_score, s.total_score
  FROM strategy g, scores_staging s
 WHERE g.strategy_cd = s.strategy_cd

RETURN 0
go
IF OBJECT_ID('dbo.scores_load') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.scores_load >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.scores_load >>>'
go
