use QER
go
IF OBJECT_ID('dbo.factor_monitor_data_retrieve') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.factor_monitor_data_retrieve
    IF OBJECT_ID('dbo.factor_monitor_data_retrieve') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.factor_monitor_data_retrieve >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.factor_monitor_data_retrieve >>>'
END
go
CREATE PROCEDURE dbo.factor_monitor_data_retrieve @DATE datetime = NULL,
                                                  @DEBUG bit = NULL
AS

DECLARE @BDATE datetime,
        @NUM int,
        @FACTOR_CD varchar(32),
        @SQL varchar(1000)

IF @DATE IS NULL
BEGIN
  SELECT @BDATE = MAX(bdate) FROM instrument_factor
   WHERE factor_id IN (SELECT factor_id FROM factor f, decode d
                        WHERE d.item = 'FACTOR MONITOR FACTOR'
                          AND f.factor_cd = d.decode)
END
ELSE
  BEGIN EXEC business_date_get @DIFF=0, @REF_DATE=@DATE, @RET_DATE=@BDATE OUTPUT END

IF @DEBUG = 1
  BEGIN SELECT '@BDATE', @BDATE END

CREATE TABLE #RESULT (
  security_id	int			NOT NULL,
  ticker		varchar(16)	NULL,
  cusip			varchar(32)	NULL,
  sedol			varchar(32)	NULL,
  isin			varchar(64)	NULL,
  mkt_cap		float		NULL
)

INSERT #RESULT (security_id)
SELECT DISTINCT p.security_id
  FROM decode d, universe_makeup p
 WHERE d.item = 'FACTOR MONITOR UNIVERSE'
   AND CONVERT(int,d.code) = p.universe_id
   AND p.universe_dt = @BDATE

UPDATE #RESULT
   SET ticker = s.ticker,
       cusip = s.cusip,
       sedol = s.sedol,
       isin = s.isin
  FROM equity_common..security s
 WHERE #RESULT.security_id = s.security_id

UPDATE #RESULT
   SET mkt_cap = p.market_cap_usd
  FROM equity_common..market_price p
 WHERE #RESULT.security_id = p.security_id
   AND p.reference_date = @BDATE

IF @DEBUG = 1
BEGIN
  SELECT '#RESULT (1)'
  SELECT * FROM #RESULT ORDER BY cusip
END

SELECT @NUM = 0
WHILE EXISTS (SELECT * FROM decode WHERE item = 'FACTOR MONITOR FACTOR' AND CONVERT(int,code) > @NUM)
BEGIN
  SELECT @NUM = MIN(CONVERT(int,code)) FROM decode WHERE item = 'FACTOR MONITOR FACTOR' AND CONVERT(int,code) > @NUM
  SELECT @FACTOR_CD = decode FROM decode WHERE item = 'FACTOR MONITOR FACTOR' AND CONVERT(int,code) = @NUM
  SELECT @SQL = 'ALTER TABLE #RESULT ADD ' + @FACTOR_CD + ' float NULL'
  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)
  SELECT @SQL = 'UPDATE #RESULT SET ' + @FACTOR_CD + ' = i.factor_value '
  SELECT @SQL = @SQL + 'FROM instrument_factor i, factor f '
  SELECT @SQL = @SQL + 'WHERE f.factor_cd = ''' + @FACTOR_CD + ''' '
  SELECT @SQL = @SQL + 'AND f.factor_id = i.factor_id '
  SELECT @SQL = @SQL + 'AND i.bdate = ''' + CONVERT(varchar,@BDATE,101) + ''' '
  SELECT @SQL = @SQL + 'AND i.source_cd = ''MQA'' '
  SELECT @SQL = @SQL + 'AND #RESULT.security_id = i.security_id'
  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)
END

IF @DEBUG = 1
BEGIN
  SELECT @SQL = 'SELECT security_id, ticker, cusip, sedol, isin, mkt_cap'
  SELECT @NUM = 0
  WHILE EXISTS (SELECT * FROM decode WHERE item = 'FACTOR MONITOR FACTOR' AND CONVERT(int,code) > @NUM)
  BEGIN
    SELECT @NUM = MIN(CONVERT(int,code)) FROM decode WHERE item = 'FACTOR MONITOR FACTOR' AND CONVERT(int,code) > @NUM
    SELECT @FACTOR_CD = decode FROM decode WHERE item = 'FACTOR MONITOR FACTOR' AND CONVERT(int,code) = @NUM
    SELECT @SQL = @SQL + ', ' + @FACTOR_CD
  END
  SELECT @SQL = @SQL + ' FROM #RESULT ORDER BY cusip'
  SELECT '@SQL', @SQL
  SELECT '#RESULT (2)'
  EXEC(@SQL)
END

SELECT @SQL = 'SELECT SUBSTRING(cusip,1,8) AS [cusip], mkt_cap / 1000000 AS [mkt_cap]'
SELECT @NUM = 0
WHILE EXISTS (SELECT * FROM decode WHERE item = 'FACTOR MONITOR FACTOR' AND CONVERT(int,code) > @NUM)
BEGIN
  SELECT @NUM = MIN(CONVERT(int,code)) FROM decode WHERE item = 'FACTOR MONITOR FACTOR' AND CONVERT(int,code) > @NUM
  SELECT @FACTOR_CD = decode FROM decode WHERE item = 'FACTOR MONITOR FACTOR' AND CONVERT(int,code) = @NUM
  SELECT @SQL = @SQL + ', ' + @FACTOR_CD
END
SELECT @SQL = @SQL + ' FROM #RESULT ORDER BY cusip'
IF @DEBUG = 1 BEGIN SELECT '@SQL', @SQL END
EXEC(@SQL)

DROP TABLE #RESULT

RETURN 0
go
IF OBJECT_ID('dbo.factor_monitor_data_retrieve') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.factor_monitor_data_retrieve >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.factor_monitor_data_retrieve >>>'
go
