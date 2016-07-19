use QER
go
IF OBJECT_ID('dbo.country_decode_update') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.country_decode_update
    IF OBJECT_ID('dbo.country_decode_update') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.country_decode_update >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.country_decode_update >>>'
END
go
CREATE PROCEDURE dbo.country_decode_update
@BDATE datetime
AS

IF @BDATE IS NULL
  BEGIN SELECT 'ERROR: @BDATE IS A REQUIRED PARAMETER' RETURN -1 END

CREATE TABLE #COUNTRY (
  country		varchar(8)		NULL,
  country_nm	varchar(128)	NULL
)

INSERT #COUNTRY
SELECT DISTINCT c.country_cd, UPPER(c.country_name)
  FROM equity_common..security y, equity_common..country c,
      (SELECT security_id FROM universe_makeup WHERE universe_dt = @BDATE
       UNION
       SELECT security_id FROM equity_common..position
        WHERE reference_date = @BDATE
          AND reference_date = effective_date
          AND acct_cd IN (SELECT DISTINCT a.acct_cd
                            FROM equity_common..account a,
                                (SELECT account_cd AS [account_cd] FROM account
                                 UNION
                                 SELECT benchmark_cd AS [account_cd] FROM benchmark) q
                           WHERE a.parent = q.account_cd OR a.acct_cd = q.account_cd)) x
 WHERE y.security_id = x.security_id
   AND y.issue_country_cd = c.country_cd
   AND y.issue_country_cd IS NOT NULL
   AND c.country_name IS NOT NULL

DELETE #COUNTRY
  FROM decode d
 WHERE d.item = 'COUNTRY'
   AND #COUNTRY.country = d.code
   AND #COUNTRY.country_nm = d.decode

DELETE decode
  FROM #COUNTRY e
 WHERE QER..decode.item = 'COUNTRY'
   AND QER..decode.code = e.country

INSERT decode
SELECT 'COUNTRY', country, country_nm
  FROM #COUNTRY

DROP TABLE #COUNTRY

RETURN 0
go
IF OBJECT_ID('dbo.country_decode_update') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.country_decode_update >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.country_decode_update >>>'
go
