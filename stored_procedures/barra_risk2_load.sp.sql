use QER
go
IF OBJECT_ID('dbo.barra_risk2_load') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.barra_risk2_load
    IF OBJECT_ID('dbo.barra_risk2_load') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.barra_risk2_load >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.barra_risk2_load >>>'
END
go
CREATE PROCEDURE dbo.barra_risk2_load @MONTH_END_DT datetime
AS

IF @MONTH_END_DT IS NULL
  BEGIN SELECT 'ERROR: @MONTH_END_DT IS A REQUIRED PARAMETER' RETURN -1 END

CREATE TABLE #WEIGHTS (
  barra_id	varchar(16)	NULL,
  ind_cd1	int		NULL,
  wgt1		float	NULL,
  ind_cd2	int		NULL,
  wgt2		float	NULL,
  ind_cd3	int		NULL,
  wgt3		float	NULL,
  ind_cd4	int		NULL,
  wgt4		float	NULL,
  ind_cd5	int		NULL,
  wgt5		float	NULL
)

INSERT #WEIGHTS
SELECT barra_id, ind_cd1, wgt1, ind_cd2, wgt2, ind_cd3, wgt3, ind_cd4, wgt4, ind_cd5, wgt5
  FROM barra_risk
 WHERE month_end_dt= @MONTH_END_DT

CREATE TABLE #WEIGHTS2 (
  barra_id varchar(16) NULL,
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

INSERT #WEIGHTS2
SELECT barra_id,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
  FROM #WEIGHTS

DECLARE @COL_NAME varchar(4),
        @WGT float,
        @BARRA_ID varchar(32)

SELECT @BARRA_ID = 0

WHILE EXISTS (SELECT * FROM #WEIGHTS WHERE barra_id > @BARRA_ID)
BEGIN
  SELECT @BARRA_ID = MIN(barra_id)
    FROM #WEIGHTS WHERE barra_id > @BARRA_ID

  SELECT @COL_NAME = ind_cd1,
         @WGT = wgt1
    FROM #WEIGHTS
   WHERE barra_id = @BARRA_ID
  IF @WGT != 0.0 BEGIN EXEC('UPDATE #WEIGHTS2 SET [' + @COL_NAME + '] = ' + @WGT + ' WHERE barra_id = ''' + @BARRA_ID + '''') END

  SELECT @COL_NAME = ind_cd2,
         @WGT = wgt2
    FROM #WEIGHTS
   WHERE barra_id = @BARRA_ID
  IF @WGT != 0.0 BEGIN EXEC('UPDATE #WEIGHTS2 SET [' + @COL_NAME + '] = ' + @WGT + ' WHERE barra_id = ''' + @BARRA_ID + '''') END

  SELECT @COL_NAME = ind_cd3,
         @WGT = wgt3
    FROM #WEIGHTS
   WHERE barra_id = @BARRA_ID
  IF @WGT != 0.0 BEGIN EXEC('UPDATE #WEIGHTS2 SET [' + @COL_NAME + '] = ' + @WGT + ' WHERE barra_id = ''' + @BARRA_ID + '''') END

  SELECT @COL_NAME = ind_cd4,
         @WGT = wgt4
    FROM #WEIGHTS
   WHERE barra_id = @BARRA_ID
  IF @WGT != 0.0 BEGIN EXEC('UPDATE #WEIGHTS2 SET [' + @COL_NAME + '] = ' + @WGT + ' WHERE barra_id = ''' + @BARRA_ID + '''') END

  SELECT @COL_NAME = ind_cd5,
         @WGT = wgt5
    FROM #WEIGHTS
   WHERE barra_id = @BARRA_ID
  IF @WGT != 0.0 BEGIN EXEC('UPDATE #WEIGHTS2 SET [' + @COL_NAME + '] = ' + @WGT + ' WHERE barra_id = ''' + @BARRA_ID + '''') END
END

DELETE barra_risk2
 WHERE month_end_dt = @MONTH_END_DT

INSERT barra_risk2
SELECT r.month_end_dt, r.barra_id, r.ticker, r.cusip, r.name,
       r.hist_beta, r.beta, r.spec_risk, r.tot_risk, r.volatility,
       r.momentum, r.size, r.size_nonlin, r.trade_act, r.growth,
       r.earn_yield, r.value, r.earn_var, r.leverage,
       r.curr_sen, r.dividend_yield, r.in_non_est_univ,
       w2.[1], w2.[2], w2.[3], w2.[4], w2.[5], w2.[6], w2.[7], w2.[8], w2.[9], w2.[10],
       w2.[11], w2.[12], w2.[13], w2.[14], w2.[15], w2.[16], w2.[17], w2.[18], w2.[19], w2.[20],
       w2.[21], w2.[22], w2.[23], w2.[24], w2.[25], w2.[26], w2.[27], w2.[28], w2.[29], w2.[30],
       w2.[31], w2.[32], w2.[33], w2.[34], w2.[35], w2.[36], w2.[37], w2.[38], w2.[39], w2.[40],
       w2.[41], w2.[42], w2.[43], w2.[44], w2.[45], w2.[46], w2.[47], w2.[48], w2.[49], w2.[50],
       w2.[51], w2.[52], w2.[53], w2.[54], w2.[55],
       r.price, r.capitalization, r.yield,
       r.in_SAP500, r.in_SAPVAL, r.in_SAPGRO, r.in_MIDCAP, r.in_MIDVAL,
       r.in_MIDGRO, r.in_SC600, r.in_SCVAL, r.in_SCGRO, r.in_E3ESTU
  FROM barra_risk r, #WEIGHTS2 w2
 WHERE r.month_end_dt = @MONTH_END_DT
   AND r.barra_id = w2.barra_id

DROP TABLE #WEIGHTS2
DROP TABLE #WEIGHTS

RETURN 0
go
IF OBJECT_ID('dbo.barra_risk2_load') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.barra_risk2_load >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.barra_risk2_load >>>'
go
