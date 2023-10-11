with ct_origem as (
  select
    ICAO_aerodromo_origem as ICAO_aeroporto,
    ICAO_empresa_area AS ICAO_cia_area,
    count(*) as cont_aer_emp
  from `concise-crane-336515.eleflow.via_2011`
  group by ICAO_aerodromo_origem, ICAO_empresa_area
),
ct_destino as (
  select
    ICAO_aerodromo_destino as ICAO_aeroporto,
    ICAO_empresa_area AS ICAO_cia_area,
    count(*) as cont_aer_emp
  from `concise-crane-336515.eleflow.via_2011`
  group by ICAO_aerodromo_destino, ICAO_empresa_area
),
ct_aeroport as (
  select * from ct_origem
  union all
  select * from ct_destino
),
sum_voos_aeroporto as (
  select 
    ICAO_aeroporto,
    ICAO_cia_area,
    SUM(ct_aeroport.cont_aer_emp) as sum_voos
from ct_aeroport
GROUP BY ICAO_aeroporto,ICAO_cia_area
order by ICAO_aeroporto, sum_voos DESC
)
SELECT
    ICAO_aeroporto,
    ICAO_cia_area,
    sum_voos,
    rank() over (partition BY ICAO_aeroporto ORDER BY sum_voos_aeroporto.sum_voos DESC) AS rank
FROM sum_voos_aeroporto
ORDER BY sum_voos_aeroporto.ICAO_aeroporto asc, sum_voos_aeroporto.sum_voos desc