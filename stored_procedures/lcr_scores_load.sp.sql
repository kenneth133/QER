use QER
go
IF OBJECT_ID('dbo.lcr_scores_load') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.lcr_scores_load
    IF OBJECT_ID('dbo.lcr_scores_load') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.lcr_scores_load >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.lcr_scores_load >>>'
END
go
CREATE PROCEDURE dbo.lcr_scores_load @FILENAME  varchar(64) = NULL,
                                     @REF_DATE datetime = NULL,
                                     @OVERWRITE varchar(1) = 'Y'
AS

IF @FILENAME IS NULL
BEGIN
  SELECT 'ERROR: @FILENAME PARAMETER MUST BE PASSED'
  RETURN -1
END

SELECT @FILENAME  = upper(@FILENAME)
SELECT @OVERWRITE = upper(@OVERWRITE)

IF @OVERWRITE NOT IN ('Y','N')
BEGIN
  SELECT 'ERROR: INVALID VALUE PASSED FOR @OVERWRITE PARAMETER'
  RETURN -1
END

DECLARE @PREV_BUS_DAY datetime

EXEC business_date_get -1, @REF_DATE, NULL, @PREV_BUS_DAY OUTPUT

CREATE TABLE #LCR_SCORES (
  priced_date		datetime	NOT NULL,
  mandate_id		int		NULL,
  sector_id		int		NOT NULL,
  ticker		varchar(16)	NOT NULL,
  sector_score		float		NULL,
  segment_score		float		NULL,
  portfolio_score	float		NULL
)

INSERT #LCR_SCORES
SELECT @PREV_BUS_DAY, NULL, m.sector_id, s.ticker, s.sector_score, s.segment_score, s.portfolio_score
  FROM QER..lcr_scores_staging s, QER..lcr_scores_map m
 WHERE m.filename = @FILENAME

IF @OVERWRITE = 'Y'
  BEGIN
    DELETE QER..lcr_scores
      FROM #LCR_SCORES s
     WHERE s.priced_date = QER..lcr_scores.priced_date
       AND s.ticker = QER..lcr_scores.ticker
       AND s.sector_id = QER..lcr_scores.sector_id
  END
ELSE
  BEGIN
    DELETE #LCR_SCORES
      FROM QER..lcr_scores s
     WHERE s.priced_date = #LCR_SCORES.priced_date
       AND s.ticker = #LCR_SCORES.ticker
       AND s.sector_id = #LCR_SCORES.sector_id
  END

INSERT QER..lcr_scores
SELECT *
  FROM #LCR_SCORES

DROP TABLE #LCR_SCORES

RETURN 0
go
IF OBJECT_ID('dbo.lcr_scores_load') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.lcr_scores_load >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.lcr_scores_load >>>'
go