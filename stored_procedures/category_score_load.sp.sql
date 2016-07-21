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
AS

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

CREATE TABLE #CATEGORY_SCORE_STAGING (
  bdate				datetime	NULL,
  strategy_cd		varchar(16)	NULL,
  security_id		int			NULL,
  ticker			varchar(32)	NULL,
  cusip				varchar(32)	NULL,
  sedol				varchar(32)	NULL,
  isin				varchar(32)	NULL,
  currency_cd		varchar(3)	NULL,
  exchange_nm		varchar(60)	NULL,
  score_level		varchar(32)	NULL,
  category_nm		varchar(64)	NULL,
  category_score	float		NULL
)

INSERT #CATEGORY_SCORE_STAGING
SELECT bdate, strategy_cd, NULL, ticker, cusip, sedol, isin, currency_cd, exchange_nm,
       score_level, category_nm, category_score
  FROM category_score_staging

EXEC security_id_update @TABLE_NAME='#CATEGORY_SCORE_STAGING'

DELETE category_score
  FROM (SELECT DISTINCT s.bdate, g.strategy_id
          FROM strategy g, #CATEGORY_SCORE_STAGING s
         WHERE g.strategy_cd = s.strategy_cd) x
 WHERE category_score.bdate = x.bdate
   AND category_score.strategy_id = x.strategy_id

INSERT category_score
      (bdate, strategy_id, security_id, score_level, category, category_score)
SELECT s.bdate, g.strategy_id, s.security_id, s.score_level, d.code, s.category_score
  FROM strategy g, #CATEGORY_SCORE_STAGING s, decode d
 WHERE g.strategy_cd = s.strategy_cd
   AND d.item = 'FACTOR_CATEGORY'
   AND d.decode = s.category_nm

DROP TABLE #CATEGORY_SCORE_STAGING

RETURN 0
go
IF OBJECT_ID('dbo.category_score_load') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.category_score_load >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.category_score_load >>>'
go
