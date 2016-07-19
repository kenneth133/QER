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
CREATE PROCEDURE dbo.scores_load
AS

CREATE TABLE #SCORES_STAGING (
  bdate				datetime	NULL,
  strategy_cd		varchar(16)	NULL,
  security_id		int			NULL,
  ticker			varchar(16)	NULL,
  cusip				varchar(32)	NULL,
  sedol				varchar(32)	NULL,
  isin				varchar(64)	NULL,
  currency_cd		varchar(3)	NULL,
  exchange_nm		varchar(40)	NULL,
  sector_score		float		NULL,
  segment_score		float		NULL,
  ss_score			float		NULL,
  universe_score	float		NULL,
  country_score		float		NULL,
  total_score		float		NULL
)

INSERT #SCORES_STAGING
SELECT bdate, strategy_cd, NULL, ticker, cusip, sedol, isin, currency_cd, exchange_nm,
       sector_score, segment_score, ss_score, universe_score, country_score, total_score
  FROM scores_staging

EXEC security_id_update @TABLE_NAME='#SCORES_STAGING'

DELETE scores
  FROM (SELECT DISTINCT s.bdate, g.strategy_id
          FROM strategy g, #SCORES_STAGING s
         WHERE g.strategy_cd = s.strategy_cd) x
 WHERE scores.bdate = x.bdate
   AND scores.strategy_id = x.strategy_id

INSERT scores (bdate, strategy_id, security_id,
       sector_score, segment_score, ss_score, universe_score, country_score, total_score)
SELECT s.bdate, g.strategy_id, s.security_id,
       s.sector_score, s.segment_score, s.ss_score, s.universe_score, s.country_score, s.total_score
  FROM strategy g, #SCORES_STAGING s
 WHERE g.strategy_cd = s.strategy_cd

DROP TABLE #SCORES_STAGING

RETURN 0
go
IF OBJECT_ID('dbo.scores_load') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.scores_load >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.scores_load >>>'
go
