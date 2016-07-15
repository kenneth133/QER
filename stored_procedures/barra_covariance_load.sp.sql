use QER
go
IF OBJECT_ID('dbo.barra_covariance_load') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.barra_covariance_load
    IF OBJECT_ID('dbo.barra_covariance_load') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.barra_covariance_load >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.barra_covariance_load >>>'
END
go
CREATE PROCEDURE dbo.barra_covariance_load @MONTH_END_DT datetime = NULL
AS

IF @MONTH_END_DT IS NULL
BEGIN
  SELECT @MONTH_END_DT = SUBSTRING(CONVERT(varchar, GETDATE(), 112), 1, 6) + '01'
  SELECT @MONTH_END_DT = DATEADD(DD, -1, @MONTH_END_DT)
END

DELETE barra_covariance
 WHERE month_end_dt = @MONTH_END_DT

INSERT barra_covariance
SELECT @MONTH_END_DT, d2.item, CONVERT(int, d2.code),
       s.VOLTILTY, s.MOMENTUM, s.SIZE, s.SIZENONL, s.TRADEACT, s.GROWTH, s.EARNYLD,
       s.VALUE, s.EARNVAR, s.LEVERAGE, s.CURRSEN, s.YIELD, s.NONESTU,
       s.MINING, s.GOLD, s.FOREST, s.CHEMICAL, s.ENGYRES,
       s.OILREF, s.OILSVCS, s.FOODBEV, s.ALCOHOL, s.TOBACCO,
       s.HOMEPROD, s.GROCERY, s.CONSDUR, s.MOTORVEH, s.APPAREL,
       s.CLOTHING, s.SPLTYRET, s.DEPTSTOR, s.CONSTRUC, s.PUBLISH,
       s.MEDIA, s.HOTELS, s.RESTRNTS, s.ENTRTAIN, s.LEISURE,
       s.ENVSVCS, s.HEAVYELC, s.HEAVYMCH, s.INDPART, s.ELECUTIL,
       s.GASUTIL, s.RAILROAD, s.AIRLINES, s.TRUCKFRT, s.MEDPROVR,
       s.MEDPRODS, s.DRUGS, s.ELECEQP, s.SEMICOND, s.CMPTRHW,
       s.CMPTRSW, s.DEFAERO, s.TELEPHON, s.WIRELESS, s.INFOSVCS,
       s.INDSVCS, s.LIFEINS, s.PRPTYINS, s.BANKS, s.THRIFTS,
       s.SECASSET, s.FINSVCS, s.INTERNET, s.EQTYREIT, s.BIOTECH
  FROM barra_covariance_staging s, decode d1, decode d2
 WHERE d1.item = 'BARRA_ABBREV'
   AND d1.code = s.NAME
   AND d1.decode = d2.decode
   AND d2.item = 'BARRA_RISK_CD'

INSERT barra_covariance
SELECT @MONTH_END_DT, m.industry_model_cd, i.industry_num,
       s.VOLTILTY, s.MOMENTUM, s.SIZE, s.SIZENONL, s.TRADEACT, s.GROWTH, s.EARNYLD,
       s.VALUE, s.EARNVAR, s.LEVERAGE, s.CURRSEN, s.YIELD, s.NONESTU,
       s.MINING, s.GOLD, s.FOREST, s.CHEMICAL, s.ENGYRES,
       s.OILREF, s.OILSVCS, s.FOODBEV, s.ALCOHOL, s.TOBACCO,
       s.HOMEPROD, s.GROCERY, s.CONSDUR, s.MOTORVEH, s.APPAREL,
       s.CLOTHING, s.SPLTYRET, s.DEPTSTOR, s.CONSTRUC, s.PUBLISH,
       s.MEDIA, s.HOTELS, s.RESTRNTS, s.ENTRTAIN, s.LEISURE,
       s.ENVSVCS, s.HEAVYELC, s.HEAVYMCH, s.INDPART, s.ELECUTIL,
       s.GASUTIL, s.RAILROAD, s.AIRLINES, s.TRUCKFRT, s.MEDPROVR,
       s.MEDPRODS, s.DRUGS, s.ELECEQP, s.SEMICOND, s.CMPTRHW,
       s.CMPTRSW, s.DEFAERO, s.TELEPHON, s.WIRELESS, s.INFOSVCS,
       s.INDSVCS, s.LIFEINS, s.PRPTYINS, s.BANKS, s.THRIFTS,
       s.SECASSET, s.FINSVCS, s.INTERNET, s.EQTYREIT, s.BIOTECH
  FROM barra_covariance_staging s, decode d, industry_model m, industry i
 WHERE d.item = 'BARRA_ABBREV'
   AND d.code = s.name
   AND d.decode = i.industry_nm
   AND i.industry_model_id = m.industry_model_id
   AND m.industry_model_cd = 'BARRA-I'

RETURN 0
go
IF OBJECT_ID('dbo.barra_covariance_load') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.barra_covariance_load >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.barra_covariance_load >>>'
go