create or replace view `logistics-metrics-488014.adr_argentina.marts_adrs_arg` as
with stagging as (
SELECT
  extract(year from FECHA) as anio,
  extract(month from FECHA) as mes,
  safe_cast(FECHA as date) as fecha,
  ADRS,
  APERTURA,
  CIERRE,
  LAG(CIERRE) OVER(PARTITION BY ADRS ORDER BY SAFE_CAST(FECHA as date) asc) AS CIERRE_ANTERIOR,
  PRECIO_MINIMO,
  PRECIO_MAXIMO,
  VOLUMEN
FROM
  `logistics-metrics-488014.adr_argentina.stg_adr_arg`


),
marts_return_daily as (
  SELECT
  anio,
  mes,
  FECHA,
  adrs,
  cierre,
  cierre_anterior,
  round(((cierre - cierre_anterior)/(cierre_anterior)),5) as variacion_pct_diaria,
  first_value(cierre) over(partition by adrs order by fecha asc)as primer_valor ,
  volumen
  FROM
  stagging
),
marts_ev_acum_volatilidad as(
select  
*,
round(cierre/primer_valor,5) as evolucion_acumulada,
round(stddev(variacion_pct_diaria) over(partition by ADRS order by fecha asc rows between 16 preceding and current row ),5) as volatilidad
from marts_return_daily
WHERE variacion_pct_diaria is not null
and cierre_anterior is not null
and primer_valor is not null
),
marts_cruce_dorado as (
SELECT 
  fecha,
  adrs,
  cierre,
  variacion_pct_diaria,
  evolucion_acumulada,
  volatilidad,
  round(avg(cierre) over(partition by adrs order by fecha asc rows between 49 preceding and current row),5) cruce_dorado_SMA50,
  round(avg(cierre) over(partition by adrs order by fecha asc rows between 199 preceding and current row),5) cruce_dorado_SMA200,
  round(volumen/avg(volumen) over(partition by adrs order by fecha asc rows between 19 preceding and current row),5) as ratio_de_volumen,
  volumen,
  CASE
    WHEN avg(cierre) over(partition by adrs order by fecha asc rows between 49 preceding and current row) > avg(cierre) over(partition by adrs order by fecha asc rows    between 199 preceding and current row) then "Tendencia alcista"
    else "Tendencia bajista"
    end as tendencia
FROM 
  marts_ev_acum_volatilidad
WHERE volatilidad is not null
)
SELECT 
  * 
FROM 
  marts_cruce_dorado
