create or replace view `logistics-metrics-488014.adr_argentina.stg_adr_arg` as 
SELECT DISTINCT
  SAFE_CAST(date as DATETIME) AS FECHA,
  SAFE_CAST(close AS FLOAT64) AS CIERRE,
  SAFE_CAST(ticker AS string) as ADRS,
  SAFE_CAST(high AS FLOAT64)  AS PRECIO_MAXIMO,
  SAFE_CAST(low AS FLOAT64) AS PRECIO_MINIMO,
  SAFE_CAST(open AS FLOAT64) AS APERTURA,
  SAFE_CAST(volume AS int64) AS VOLUMEN
FROM 
  `logistics-metrics-488014.adr_argentina.raw_adr_history` 
WHERE date IS NOT NULL
  AND ticker is not null
  AND volume is not null
