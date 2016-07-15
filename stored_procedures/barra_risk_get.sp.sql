use QER
go
IF OBJECT_ID('dbo.barra_risk_get') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.barra_risk_get
    IF OBJECT_ID('dbo.barra_risk_get') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.barra_risk_get >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.barra_risk_get >>>'
END
go
CREATE PROCEDURE dbo.barra_risk_get @MONTH_END_DT datetime
AS

IF @MONTH_END_DT IS NULL
  BEGIN SELECT 'ERROR: @MONTH_END_DT IS A REQUIRED PARAMETER' RETURN -1 END

CREATE TABLE #WEIGHTS (
  cusip		varchar(32)	NULL,
  ind_cd1	int		null,
  wgt1		float		null,
  ind_cd2	int		null,
  wgt2		float		null,
  ind_cd3	int		null,
  wgt3		float		null,
  ind_cd4	int		null,
  wgt4		float		null,
  ind_cd5	int		null,
  wgt5		float		null
)

INSERT #WEIGHTS
SELECT cusip, ind_cd1, wgt1, ind_cd2, wgt2, ind_cd3, wgt3, ind_cd4, wgt4, ind_cd5, wgt5
  FROM barra_risk
 WHERE month_end_dt= @MONTH_END_DT

CREATE TABLE #RESULT (
  cusip			varchar(32)	NULL,
  volatility		float		NULL,
  momentum		float		NULL,
  size			float		NULL,
  size_nonlin		float		NULL,
  trade_act		float		NULL,
  growth		float		NULL,
  earn_yield		float		NULL,
  value			float		NULL,
  earn_var		float		NULL,
  leverage		float		NULL,
  curr_sen		float		NULL,
  dividend_yield	float		NULL,
  in_non_est_univ	bit		NULL,
  [1] float NULL, [2] float NULL, [3] float NULL, [4] float NULL, [5] float NULL,
  [6] float NULL, [7] float NULL, [8] float NULL, [9] float NULL, [10] float NULL,
  [11] float NULL, [12] float NULL, [13] float NULL, [14] float NULL, [15] float NULL,
  [16] float NULL, [17] float NULL, [18] float NULL, [19] float NULL, [20] float NULL,
  [21] float NULL, [22] float NULL, [23] float NULL, [24] float NULL, [25] float NULL,
  [26] float NULL, [27] float NULL, [28] float NULL, [29] float NULL, [30] float NULL,
  [31] float NULL, [32] float NULL, [33] float NULL, [34] float NULL, [35] float NULL,
  [36] float NULL, [37] float NULL, [38] float NULL, [39] float NULL, [40] float NULL,
  [41] float NULL, [42] float NULL, [43] float NULL, [44] float NULL, [45] float NULL,
  [46] float NULL, [47] float NULL, [48] float NULL, [49] float NULL, [50] float NULL,
  [51] float NULL, [52] float NULL, [53] float NULL, [54] float NULL, [55] float NULL
)

INSERT #RESULT
SELECT cusip, volatility, momentum, size, size_nonlin, trade_act, growth, earn_yield,
       value, earn_var, leverage, curr_sen, dividend_yield, in_non_est_univ,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
  FROM barra_risk
 WHERE month_end_dt= @MONTH_END_DT

DECLARE @COL_NAME varchar(4),
        @WGT float,
        @CUSIP varchar(32)

SELECT @CUSIP = 0

WHILE EXISTS (SELECT * FROM #WEIGHTS WHERE cusip > @CUSIP)
BEGIN
  SELECT @CUSIP = MIN(cusip)
    FROM #WEIGHTS WHERE cusip > @CUSIP

  SELECT @COL_NAME = ind_cd1,
         @WGT = wgt1
    FROM #WEIGHTS
   WHERE cusip = @CUSIP
  IF @WGT != 0.0 BEGIN EXEC('UPDATE #RESULT SET [' + @COL_NAME + '] = ' + @WGT + ' WHERE cusip = ''' + @CUSIP + '''') END

  SELECT @COL_NAME = ind_cd2,
         @WGT = wgt2
    FROM #WEIGHTS
   WHERE cusip = @CUSIP
  IF @WGT != 0.0 BEGIN EXEC('UPDATE #RESULT SET [' + @COL_NAME + '] = ' + @WGT + ' WHERE cusip = ''' + @CUSIP + '''') END

  SELECT @COL_NAME = ind_cd3,
         @WGT = wgt3
    FROM #WEIGHTS
   WHERE cusip = @CUSIP
  IF @WGT != 0.0 BEGIN EXEC('UPDATE #RESULT SET [' + @COL_NAME + '] = ' + @WGT + ' WHERE cusip = ''' + @CUSIP + '''') END

  SELECT @COL_NAME = ind_cd4,
         @WGT = wgt4
    FROM #WEIGHTS
   WHERE cusip = @CUSIP
  IF @WGT != 0.0 BEGIN EXEC('UPDATE #RESULT SET [' + @COL_NAME + '] = ' + @WGT + ' WHERE cusip = ''' + @CUSIP + '''') END

  SELECT @COL_NAME = ind_cd5,
         @WGT = wgt5
    FROM #WEIGHTS
   WHERE cusip = @CUSIP
  IF @WGT != 0.0 BEGIN EXEC('UPDATE #RESULT SET [' + @COL_NAME + '] = ' + @WGT + ' WHERE cusip = ''' + @CUSIP + '''') END
END

SELECT * FROM #RESULT ORDER BY cusip

DROP TABLE #RESULT
DROP TABLE #WEIGHTS

RETURN 0
go
IF OBJECT_ID('dbo.barra_risk_get') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.barra_risk_get >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.barra_risk_get >>>'
go
