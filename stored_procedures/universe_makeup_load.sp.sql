use QER
go
IF OBJECT_ID('dbo.universe_makeup_load') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.universe_makeup_load
    IF OBJECT_ID('dbo.universe_makeup_load') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.universe_makeup_load >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.universe_makeup_load >>>'
END
go
CREATE PROCEDURE dbo.universe_makeup_load
@MODE varchar(16) = 'RELOAD',
@IDENTIFIER varchar(16) = 'CUSIP'
AS

SELECT @MODE = UPPER(@MODE)
SELECT @IDENTIFIER = UPPER(@IDENTIFIER)

IF @MODE NOT IN ('APPEND', 'OVERWRITE', 'RELOAD')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @MODE PARAMETER' RETURN -1 END
IF @MODE IN ('APPEND', 'OVERWRITE') AND @IDENTIFIER NOT IN ('TICKER', 'CUSIP', 'SEDOL', 'ISIN')
  BEGIN SELECT 'ERROR: INVALID VALUE PASSED FOR @IDENTIFIER PARAMETER' RETURN -1 END

DELETE universe_makeup_staging
 WHERE cusip IS NULL
   AND ticker IS NULL
   AND sedol IS NULL
   AND isin IS NULL

IF @MODE = 'RELOAD'
BEGIN
  DELETE universe_makeup
    FROM (SELECT DISTINCT s.universe_dt, d.universe_id
            FROM universe_def d, universe_makeup_staging s
           WHERE d.universe_cd = s.universe_cd) x
   WHERE universe_makeup.universe_dt = x.universe_dt
     AND universe_makeup.universe_id = x.universe_id
END
ELSE IF @MODE = 'OVERWRITE'
BEGIN
  IF @IDENTIFIER = 'TICKER'
  BEGIN
    DELETE universe_makeup
      FROM universe_def d, universe_makeup_staging s
     WHERE d.universe_cd = s.universe_cd
       AND universe_makeup.universe_id = d.universe_id
       AND universe_makeup.universe_dt = s.universe_dt
       AND universe_makeup.ticker = s.ticker
  END
  ELSE IF @IDENTIFIER = 'CUSIP'
  BEGIN
    DELETE universe_makeup
      FROM universe_def d, universe_makeup_staging s
     WHERE d.universe_cd = s.universe_cd
       AND universe_makeup.universe_id = d.universe_id
       AND universe_makeup.universe_dt = s.universe_dt
       AND universe_makeup.cusip = s.cusip
  END
  ELSE IF @IDENTIFIER = 'SEDOL'
  BEGIN
    DELETE universe_makeup
      FROM universe_def d, universe_makeup_staging s
     WHERE d.universe_cd = s.universe_cd
       AND universe_makeup.universe_id = d.universe_id
       AND universe_makeup.universe_dt = s.universe_dt
       AND universe_makeup.sedol = s.sedol
  END
  ELSE IF @IDENTIFIER = 'ISIN'
  BEGIN
    DELETE universe_makeup
      FROM universe_def d, universe_makeup_staging s
     WHERE d.universe_cd = s.universe_cd
       AND universe_makeup.universe_id = d.universe_id
       AND universe_makeup.universe_dt = s.universe_dt
       AND universe_makeup.isin = s.isin
  END
END
ELSE IF @MODE = 'APPEND'
BEGIN
  IF @IDENTIFIER = 'TICKER'
  BEGIN
    DELETE universe_makeup_staging
      FROM universe_def d, universe_makeup p
     WHERE d.universe_id = p.universe_id
       AND universe_makeup_staging.universe_cd = d.universe_cd
       AND universe_makeup_staging.universe_dt = p.universe_dt
       AND universe_makeup_staging.ticker = p.ticker
  END
  ELSE IF @IDENTIFIER = 'CUSIP'
  BEGIN
    DELETE universe_makeup_staging
      FROM universe_def d, universe_makeup p
     WHERE d.universe_id = p.universe_id
       AND universe_makeup_staging.universe_cd = d.universe_cd
       AND universe_makeup_staging.universe_dt = p.universe_dt
       AND universe_makeup_staging.cusip = p.cusip
  END
  ELSE IF @IDENTIFIER = 'SEDOL'
  BEGIN
    DELETE universe_makeup_staging
      FROM universe_def d, universe_makeup p
     WHERE d.universe_id = p.universe_id
       AND universe_makeup_staging.universe_cd = d.universe_cd
       AND universe_makeup_staging.universe_dt = p.universe_dt
       AND universe_makeup_staging.sedol = p.sedol
  END
  ELSE IF @IDENTIFIER = 'ISIN'
  BEGIN
    DELETE universe_makeup_staging
      FROM universe_def d, universe_makeup p
     WHERE d.universe_id = p.universe_id
       AND universe_makeup_staging.universe_cd = d.universe_cd
       AND universe_makeup_staging.universe_dt = p.universe_dt
       AND universe_makeup_staging.isin = p.isin
  END
END

INSERT universe_makeup
      (universe_dt, universe_id, mqa_id, ticker, cusip, sedol, isin, gv_key, weight)
SELECT s.universe_dt, d.universe_id, s.mqa_id, s.ticker, s.cusip, s.sedol, s.isin, s.gv_key, s.weight
  FROM universe_def d, universe_makeup_staging s
 WHERE d.universe_cd = s.universe_cd

RETURN 0
go
IF OBJECT_ID('dbo.universe_makeup_load') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.universe_makeup_load >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.universe_makeup_load >>>'
go