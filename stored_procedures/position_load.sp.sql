use QER
go
IF OBJECT_ID('dbo.position_load') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.position_load
    IF OBJECT_ID('dbo.position_load') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.position_load >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.position_load >>>'
END
go
CREATE PROCEDURE dbo.position_load @BDATE datetime = NULL
AS

IF @BDATE IS NULL
BEGIN
  EXEC QER..business_date_get @DIFF=-1, @RET_DATE=@BDATE OUTPUT
END

CREATE TABLE #POSITION1 (
  account_cd	varchar(32)	NOT NULL,
  security_id	int		NOT NULL,
  units		float		NOT NULL
)

INSERT #POSITION1
SELECT acct_cd, security_id, SUM(quantity)
  FROM equity_common..position
 WHERE reference_date = @BDATE
   AND acct_cd IN (SELECT account_cd FROM QER..account
                   UNION
                   SELECT account_child FROM QER..account_sleeve)
 GROUP BY acct_cd, security_id

DECLARE @BDATE_NEXT datetime
EXEC QER..business_date_get @DIFF=1, @REF_DATE=@BDATE, @RET_DATE=@BDATE_NEXT OUTPUT

DECLARE @USD_SEC_ID int
SELECT @USD_SEC_ID = MAX(security_id)
  FROM equity_common..vwSecurity_identifier
 WHERE ticker = '_USD'

INSERT #POSITION1
SELECT port, @USD_SEC_ID, SUM(net_invest_cash_balance)
  FROM equity_common..STG_cash_mutual_fund
 WHERE process_date = @BDATE_NEXT
   AND port IN (SELECT account_cd FROM QER..account
                UNION
                SELECT account_child FROM QER..account_sleeve)
 GROUP BY port

CREATE TABLE #POSITION2 (
  account_cd	varchar(32)	NOT NULL,
  security_id	int		NOT NULL,
  units		float		NOT NULL
)

INSERT #POSITION2
SELECT s.account_parent, p1.security_id, SUM(p1.units)
  FROM #POSITION1 p1, QER..account_sleeve s
 WHERE s.account_child = p1.account_cd
 GROUP BY s.account_parent, p1.security_id

INSERT #POSITION2
SELECT account_cd, security_id, SUM(units)
  FROM #POSITION1
 WHERE account_cd IN (SELECT DISTINCT account_cd FROM account)
 GROUP BY account_cd, security_id

DELETE QER..position
 WHERE QER..position.bdate = @BDATE
   AND QER..position.account_cd IN (SELECT DISTINCT account_cd FROM #POSITION2)

INSERT QER..position
      (bdate, account_cd, cusip, sedol, isin, units)
SELECT @BDATE, p2.account_cd,
       CASE WHEN SUBSTRING(i.cusip, 1, 8) = '' THEN NULL ELSE SUBSTRING(i.cusip, 1, 8) END,
       CASE WHEN i.sedol = '' THEN NULL ELSE i.sedol END,
       CASE WHEN i.isin = '' THEN NULL ELSE i.isin END,
       p2.units
  FROM #POSITION2 p2, equity_common..vwSecurity_identifier i
 WHERE p2.security_id = i.security_id
   AND p2.units != 0.0

DROP TABLE #POSITION1
DROP TABLE #POSITION2

RETURN 0
go
IF OBJECT_ID('dbo.position_load') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.position_load >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.position_load >>>'
go
