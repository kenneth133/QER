use QER
go
IF OBJECT_ID('dbo.security_id_update') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.security_id_update
    IF OBJECT_ID('dbo.security_id_update') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.security_id_update >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.security_id_update >>>'
END
go
CREATE PROCEDURE dbo.security_id_update
@TABLE_NAME varchar(64),
@DATE_COL varchar(32) = 'bdate',
@PRECEDENCE varchar(64) = 'cusip, sedol, ticker, isin',
@DEBUG bit = NULL
AS

DECLARE @SQL varchar(1000),
        @INDEX1 int,
        @INDEX2 int,
        @IDENT1 varchar(16),
        @IDENT2 varchar(16),
        @IDENT3 varchar(16),
        @IDENT4 varchar(16)

SELECT @PRECEDENCE = LOWER(@PRECEDENCE)
SELECT @PRECEDENCE = REPLACE(@PRECEDENCE, ' ', '')

SELECT @INDEX1 = CHARINDEX(',', @PRECEDENCE, 1)

IF @INDEX1 = 0 BEGIN SELECT @IDENT1 = @PRECEDENCE END
ELSE BEGIN SELECT @IDENT1 = SUBSTRING(@PRECEDENCE, 1, @INDEX1-1) END

IF @INDEX1 != 0
BEGIN
  SELECT @INDEX2 = CHARINDEX(',', @PRECEDENCE, @INDEX1+1)

  IF @INDEX2 = 0 BEGIN SELECT @IDENT2 = SUBSTRING(@PRECEDENCE, @INDEX1+1, LEN(@PRECEDENCE)) END
  ELSE BEGIN SELECT @IDENT2 = SUBSTRING(@PRECEDENCE, @INDEX1+1, @INDEX2-@INDEX1-1) END

  SELECT @INDEX1 = @INDEX2
END

IF @INDEX1 != 0
BEGIN
  SELECT @INDEX2 = CHARINDEX(',', @PRECEDENCE, @INDEX1+1)

  IF @INDEX2 = 0 BEGIN SELECT @IDENT3 = SUBSTRING(@PRECEDENCE, @INDEX1+1, LEN(@PRECEDENCE)) END
  ELSE BEGIN SELECT @IDENT3 = SUBSTRING(@PRECEDENCE, @INDEX1+1, @INDEX2-@INDEX1-1) END

  SELECT @INDEX1 = @INDEX2
END

IF @INDEX1 != 0
BEGIN
  SELECT @INDEX2 = CHARINDEX(',', @PRECEDENCE, @INDEX1+1)

  IF @INDEX2 = 0 BEGIN SELECT @IDENT4 = SUBSTRING(@PRECEDENCE, @INDEX1+1, LEN(@PRECEDENCE)) END
  ELSE BEGIN SELECT @IDENT4 = SUBSTRING(@PRECEDENCE, @INDEX1+1, @INDEX2-@INDEX1-1) END
END

SELECT @SQL = 'UPDATE '+@TABLE_NAME+' SET cusip = equity_common.dbo.fnCusipIncludeCheckDigit(cusip)'
IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
EXEC(@SQL)

SELECT @SQL = 'UPDATE '+@TABLE_NAME+' SET sedol = equity_common.dbo.fnSedolIncludeCheckDigit(sedol)'
IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
EXEC(@SQL)

IF @IDENT1 IN ('ticker', 'cusip', 'sedol', 'isin')
BEGIN
  SELECT @SQL = 'UPDATE '+@TABLE_NAME+' '
  SELECT @SQL = @SQL + 'SET security_id = s.security_id '
  SELECT @SQL = @SQL + 'FROM equity_common..security s, equity_common..decode d '
  SELECT @SQL = @SQL + 'WHERE '+@TABLE_NAME+'.security_id IS NULL '
  IF @IDENT1 = 'isin'
    BEGIN SELECT @SQL = @SQL + 'AND SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(s.isin,1,12) ' END
  ELSE
    BEGIN SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT1+' = s.'+@IDENT1+' ' END
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT1+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND s.'+@IDENT1+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND d.item_name = ''EXCHANGE'' '
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.exchange_nm = d.item_value '
  SELECT @SQL = @SQL + 'AND d.decode = s.list_exch_cd'

  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)

  SELECT @SQL = 'UPDATE '+@TABLE_NAME+' '
  SELECT @SQL = @SQL + 'SET security_id = s.security_id '
  SELECT @SQL = @SQL + 'FROM equity_common..security s '
  SELECT @SQL = @SQL + 'WHERE '+@TABLE_NAME+'.security_id IS NULL '
  IF @IDENT1 = 'isin'
    BEGIN SELECT @SQL = @SQL + 'AND SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(s.isin,1,12) ' END
  ELSE
    BEGIN SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT1+' = s.'+@IDENT1+' ' END
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT1+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND s.'+@IDENT1+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.currency_cd = s.local_ccy_cd'

  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)

  SELECT @SQL = 'UPDATE '+@TABLE_NAME+' '
  SELECT @SQL = @SQL + 'SET security_id = s.security_id '
  SELECT @SQL = @SQL + 'FROM equity_common..security s '
  SELECT @SQL = @SQL + 'WHERE '+@TABLE_NAME+'.security_id IS NULL '
  IF @IDENT1 = 'isin'
    BEGIN SELECT @SQL = @SQL + 'AND SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(s.isin,1,12) ' END
  ELSE
    BEGIN SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT1+' = s.'+@IDENT1+' ' END
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT1+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND s.'+@IDENT1+' IS NOT NULL'

  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)

  SELECT @SQL = 'UPDATE '+@TABLE_NAME+' '
  SELECT @SQL = @SQL + 'SET security_id = c.security_id '
  SELECT @SQL = @SQL + 'FROM equity_common..security_changes c '
  SELECT @SQL = @SQL + 'WHERE '+@TABLE_NAME+'.security_id IS NULL '
  SELECT @SQL = @SQL + 'AND c.column_name = '''+@IDENT1+''' '
  IF @IDENT1 = 'isin'
  BEGIN
    SELECT @SQL = @SQL + 'AND (SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(c.old_value,1,12) OR '
    SELECT @SQL = @SQL + 'SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(c.new_value,1,12)) '
  END
  ELSE
  BEGIN
    SELECT @SQL = @SQL + 'AND ('+@TABLE_NAME+'.'+@IDENT1+' = c.old_value OR '
    SELECT @SQL = @SQL + @TABLE_NAME+'.'+@IDENT1+' = c.new_value) '
  END
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@DATE_COL+' >= c.reference_date'

  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)
END

IF @IDENT2 IN ('ticker', 'cusip', 'sedol', 'isin')
BEGIN
  SELECT @SQL = 'UPDATE '+@TABLE_NAME+' '
  SELECT @SQL = @SQL + 'SET security_id = s.security_id '
  SELECT @SQL = @SQL + 'FROM equity_common..security s, equity_common..decode d '
  SELECT @SQL = @SQL + 'WHERE '+@TABLE_NAME+'.security_id IS NULL '
  IF @IDENT2 = 'isin'
    BEGIN SELECT @SQL = @SQL + 'AND SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(s.isin,1,12) ' END
  ELSE
    BEGIN SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT2+' = s.'+@IDENT2+' ' END
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT2+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND s.'+@IDENT2+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND d.item_name = ''EXCHANGE'' '
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.exchange_nm = d.item_value '
  SELECT @SQL = @SQL + 'AND d.decode = s.list_exch_cd'

  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)

  SELECT @SQL = 'UPDATE '+@TABLE_NAME+' '
  SELECT @SQL = @SQL + 'SET security_id = s.security_id '
  SELECT @SQL = @SQL + 'FROM equity_common..security s '
  SELECT @SQL = @SQL + 'WHERE '+@TABLE_NAME+'.security_id IS NULL '
  IF @IDENT2 = 'isin'
    BEGIN SELECT @SQL = @SQL + 'AND SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(s.isin,1,12) ' END
  ELSE
    BEGIN SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT2+' = s.'+@IDENT2+' ' END
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT2+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND s.'+@IDENT2+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.currency_cd = s.local_ccy_cd'

  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)

  SELECT @SQL = 'UPDATE '+@TABLE_NAME+' '
  SELECT @SQL = @SQL + 'SET security_id = s.security_id '
  SELECT @SQL = @SQL + 'FROM equity_common..security s '
  SELECT @SQL = @SQL + 'WHERE '+@TABLE_NAME+'.security_id IS NULL '
  IF @IDENT2 = 'isin'
    BEGIN SELECT @SQL = @SQL + 'AND SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(s.isin,1,12) ' END
  ELSE
    BEGIN SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT2+' = s.'+@IDENT2+' ' END
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT2+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND s.'+@IDENT2+' IS NOT NULL'

  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)

  SELECT @SQL = 'UPDATE '+@TABLE_NAME+' '
  SELECT @SQL = @SQL + 'SET security_id = c.security_id '
  SELECT @SQL = @SQL + 'FROM equity_common..security_changes c '
  SELECT @SQL = @SQL + 'WHERE '+@TABLE_NAME+'.security_id IS NULL '
  SELECT @SQL = @SQL + 'AND c.column_name = '''+@IDENT2+''' '
  IF @IDENT2 = 'isin'
  BEGIN
    SELECT @SQL = @SQL + 'AND (SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(c.old_value,1,12) OR '
    SELECT @SQL = @SQL + 'SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(c.new_value,1,12)) '
  END
  ELSE
  BEGIN
    SELECT @SQL = @SQL + 'AND ('+@TABLE_NAME+'.'+@IDENT2+' = c.old_value OR '
    SELECT @SQL = @SQL + @TABLE_NAME+'.'+@IDENT2+' = c.new_value) '
  END
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@DATE_COL+' >= c.reference_date'

  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)
END

IF @IDENT3 IN ('ticker', 'cusip', 'sedol', 'isin')
BEGIN
  SELECT @SQL = 'UPDATE '+@TABLE_NAME+' '
  SELECT @SQL = @SQL + 'SET security_id = s.security_id '
  SELECT @SQL = @SQL + 'FROM equity_common..security s, equity_common..decode d '
  SELECT @SQL = @SQL + 'WHERE '+@TABLE_NAME+'.security_id IS NULL '
  IF @IDENT3 = 'isin'
    BEGIN SELECT @SQL = @SQL + 'AND SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(s.isin,1,12) ' END
  ELSE
    BEGIN SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT3+' = s.'+@IDENT3+' ' END
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT3+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND s.'+@IDENT3+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND d.item_name = ''EXCHANGE'' '
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.exchange_nm = d.item_value '
  SELECT @SQL = @SQL + 'AND d.decode = s.list_exch_cd'

  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)

  SELECT @SQL = 'UPDATE '+@TABLE_NAME+' '
  SELECT @SQL = @SQL + 'SET security_id = s.security_id '
  SELECT @SQL = @SQL + 'FROM equity_common..security s '
  SELECT @SQL = @SQL + 'WHERE '+@TABLE_NAME+'.security_id IS NULL '
  IF @IDENT3 = 'isin'
    BEGIN SELECT @SQL = @SQL + 'AND SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(s.isin,1,12) ' END
  ELSE
    BEGIN SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT3+' = s.'+@IDENT3+' ' END
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT3+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND s.'+@IDENT3+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.currency_cd = s.local_ccy_cd'

  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)

  SELECT @SQL = 'UPDATE '+@TABLE_NAME+' '
  SELECT @SQL = @SQL + 'SET security_id = s.security_id '
  SELECT @SQL = @SQL + 'FROM equity_common..security s '
  SELECT @SQL = @SQL + 'WHERE '+@TABLE_NAME+'.security_id IS NULL '
  IF @IDENT3 = 'isin'
    BEGIN SELECT @SQL = @SQL + 'AND SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(s.isin,1,12) ' END
  ELSE
    BEGIN SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT3+' = s.'+@IDENT3+' ' END
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT3+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND s.'+@IDENT3+' IS NOT NULL'

  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)

  SELECT @SQL = 'UPDATE '+@TABLE_NAME+' '
  SELECT @SQL = @SQL + 'SET security_id = c.security_id '
  SELECT @SQL = @SQL + 'FROM equity_common..security_changes c '
  SELECT @SQL = @SQL + 'WHERE '+@TABLE_NAME+'.security_id IS NULL '
  SELECT @SQL = @SQL + 'AND c.column_name = '''+@IDENT3+''' '
  IF @IDENT3 = 'isin'
  BEGIN
    SELECT @SQL = @SQL + 'AND (SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(c.old_value,1,12) OR '
    SELECT @SQL = @SQL + 'SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(c.new_value,1,12)) '
  END
  ELSE
  BEGIN
    SELECT @SQL = @SQL + 'AND ('+@TABLE_NAME+'.'+@IDENT3+' = c.old_value OR '
    SELECT @SQL = @SQL + @TABLE_NAME+'.'+@IDENT3+' = c.new_value) '
  END
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@DATE_COL+' >= c.reference_date'

  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)
END

IF @IDENT4 IN ('ticker', 'cusip', 'sedol', 'isin')
BEGIN
  SELECT @SQL = 'UPDATE '+@TABLE_NAME+' '
  SELECT @SQL = @SQL + 'SET security_id = s.security_id '
  SELECT @SQL = @SQL + 'FROM equity_common..security s, equity_common..decode d '
  SELECT @SQL = @SQL + 'WHERE '+@TABLE_NAME+'.security_id IS NULL '
  IF @IDENT4 = 'isin'
    BEGIN SELECT @SQL = @SQL + 'AND SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(s.isin,1,12) ' END
  ELSE
    BEGIN SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT4+' = s.'+@IDENT4+' ' END
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT4+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND s.'+@IDENT4+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND d.item_name = ''EXCHANGE'' '
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.exchange_nm = d.item_value '
  SELECT @SQL = @SQL + 'AND d.decode = s.list_exch_cd'

  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)

  SELECT @SQL = 'UPDATE '+@TABLE_NAME+' '
  SELECT @SQL = @SQL + 'SET security_id = s.security_id '
  SELECT @SQL = @SQL + 'FROM equity_common..security s '
  SELECT @SQL = @SQL + 'WHERE '+@TABLE_NAME+'.security_id IS NULL '
  IF @IDENT4 = 'isin'
    BEGIN SELECT @SQL = @SQL + 'AND SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(s.isin,1,12) ' END
  ELSE
    BEGIN SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT4+' = s.'+@IDENT4+' ' END
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT4+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND s.'+@IDENT4+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.currency_cd = s.local_ccy_cd'

  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)

  SELECT @SQL = 'UPDATE '+@TABLE_NAME+' '
  SELECT @SQL = @SQL + 'SET security_id = s.security_id '
  SELECT @SQL = @SQL + 'FROM equity_common..security s '
  SELECT @SQL = @SQL + 'WHERE '+@TABLE_NAME+'.security_id IS NULL '
  IF @IDENT4 = 'isin'
    BEGIN SELECT @SQL = @SQL + 'AND SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(s.isin,1,12) ' END
  ELSE
    BEGIN SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT4+' = s.'+@IDENT4+' ' END
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@IDENT4+' IS NOT NULL '
  SELECT @SQL = @SQL + 'AND s.'+@IDENT4+' IS NOT NULL'

  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)

  SELECT @SQL = 'UPDATE '+@TABLE_NAME+' '
  SELECT @SQL = @SQL + 'SET security_id = c.security_id '
  SELECT @SQL = @SQL + 'FROM equity_common..security_changes c '
  SELECT @SQL = @SQL + 'WHERE '+@TABLE_NAME+'.security_id IS NULL '
  SELECT @SQL = @SQL + 'AND c.column_name = '''+@IDENT4+''' '
  IF @IDENT4 = 'isin'
  BEGIN
    SELECT @SQL = @SQL + 'AND (SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(c.old_value,1,12) OR '
    SELECT @SQL = @SQL + 'SUBSTRING('+@TABLE_NAME+'.isin,1,12) = SUBSTRING(c.new_value,1,12)) '
  END
  ELSE
  BEGIN
    SELECT @SQL = @SQL + 'AND ('+@TABLE_NAME+'.'+@IDENT4+' = c.old_value OR '
    SELECT @SQL = @SQL + @TABLE_NAME+'.'+@IDENT4+' = c.new_value) '
  END
  SELECT @SQL = @SQL + 'AND '+@TABLE_NAME+'.'+@DATE_COL+' >= c.reference_date'

  IF @DEBUG=1 BEGIN SELECT '@SQL', @SQL END
  EXEC(@SQL)
END

RETURN 0
go
IF OBJECT_ID('dbo.security_id_update') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.security_id_update >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.security_id_update >>>'
go
