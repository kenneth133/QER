use QER
go
IF OBJECT_ID('dbo.dummy_data_delete') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.dummy_data_delete
    IF OBJECT_ID('dbo.dummy_data_delete') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.dummy_data_delete >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.dummy_data_delete >>>'
END
go
CREATE PROCEDURE dbo.dummy_data_delete
AS

DELETE QER..rank_output
  FROM QER..rank_inputs i
 WHERE QER..rank_output.rank_event_id = i.rank_event_id
   AND i.factor_id IN (SELECT factor_id FROM QER..factor WHERE factor_cd = 'DUMMY')

DELETE QER..instrument_factor
 WHERE factor_id IN (SELECT factor_id FROM QER..factor WHERE factor_cd = 'DUMMY')

RETURN 0
go
IF OBJECT_ID('dbo.dummy_data_delete') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.dummy_data_delete >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.dummy_data_delete >>>'
go
