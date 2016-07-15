use QER
go
IF OBJECT_ID('dbo.rpt_prm_get_segment') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.rpt_prm_get_segment
    IF OBJECT_ID('dbo.rpt_prm_get_segment') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.rpt_prm_get_segment >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.rpt_prm_get_segment >>>'
END
go
CREATE PROCEDURE dbo.rpt_prm_get_segment @SECTOR_ID int
AS

SELECT segment_id, segment_nm
  FROM segment_def
 WHERE sector_id = @SECTOR_ID
 ORDER BY segment_nm

RETURN 0
go
IF OBJECT_ID('dbo.rpt_prm_get_segment') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.rpt_prm_get_segment >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.rpt_prm_get_segment >>>'
go
