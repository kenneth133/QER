use QER
go
IF OBJECT_ID('dbo.lcr_scores_load_confirm') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.lcr_scores_load_confirm
    IF OBJECT_ID('dbo.lcr_scores_load_confirm') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.lcr_scores_load_confirm >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.lcr_scores_load_confirm >>>'
END
go
CREATE PROCEDURE dbo.lcr_scores_load_confirm
AS

DECLARE @PREV_BUS_DAY datetime

EXEC business_date_get -1, NULL, NULL, @PREV_BUS_DAY OUTPUT

CREATE TABLE #LCR_SCORES_COUNT (
  sector_id		int		NOT NULL,
  filename		varchar(64)	NOT NULL,
  num_rows		int		NULL
)

INSERT #LCR_SCORES_COUNT
SELECT m.sector_id, m.filename, count(*)
  FROM QER..lcr_scores s, QER..lcr_scores_map m
 WHERE s.sector_id = m.sector_id
   AND s.priced_date = @PREV_BUS_DAY
 GROUP BY m.sector_id, m.filename

INSERT #LCR_SCORES_COUNT
SELECT sector_id, filename, 0
  FROM QER..lcr_scores_map
 WHERE filename NOT IN (SELECT filename FROM #LCR_SCORES_COUNT)

SELECT filename, num_rows
  FROM #LCR_SCORES_COUNT

DROP TABLE #LCR_SCORES_COUNT

RETURN 0
go
IF OBJECT_ID('dbo.lcr_scores_load_confirm') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.lcr_scores_load_confirm >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.lcr_scores_load_confirm >>>'
go