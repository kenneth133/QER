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

/*
NOTE:
FOR DEBUGGING PURPOSES, DO NOT DELETE DUMMY DATA FROM rank_inputs AND rank_output TABLES
*/

DELETE instrument_factor
 WHERE bdate = bdate
   AND factor_id IN (SELECT factor_id FROM factor WHERE factor_cd = 'DUMMY')

RETURN 0
go
IF OBJECT_ID('dbo.dummy_data_delete') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.dummy_data_delete >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.dummy_data_delete >>>'
go
