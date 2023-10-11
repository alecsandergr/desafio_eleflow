-- - Para cada aeroporto trazer a companhia aérea com maior atuação no ano com as seguintes informações:
--     - Nome do Aeroporto
--     - ICAO do Aeroporto
--     - Razão social da Companhia Aérea
--     - Quantidade de Rotas à partir daquele aeroporto
--     - Quantidade de Rotas com destino àquele aeroporto
--     - Quantidade total de pousos e decolagens naquele aeroporto

CREATE OR REPLACE VIEW `concise-crane-336515.eleflow.fav_aero_cia_area` AS 

WITH ct_origem AS (
  SELECT
    ICAO_aerodromo_origem AS ICAO_aeroporto,
    ICAO_empresa_area AS ICAO_cia_area,
    COUNT(*) AS cont_aero_cia_area_orig
  FROM `concise-crane-336515.eleflow.silver_VRA`
  GROUP BY ICAO_aerodromo_origem, ICAO_empresa_area
),
ct_destino AS (
  SELECT
    ICAO_aerodromo_destino AS ICAO_aeroporto,
    ICAO_empresa_area AS ICAO_cia_area,
    COUNT(*) AS cont_aero_cia_area_dest
  FROM `concise-crane-336515.eleflow.silver_VRA`
  GROUP BY ICAO_aerodromo_destino, ICAO_empresa_area
),
ct_orig_dest AS (
  SELECT
    ICAO_aeroporto,
    ICAO_cia_area,
    IFNULL(t1.cont_aero_cia_area_orig,0) AS voos_orig,
    IFNULL(t2.cont_aero_cia_area_dest,0) AS voos_dest
  FROM ct_origem AS t1
  FULL JOIN ct_destino AS t2
  USING (ICAO_aeroporto, ICAO_cia_area)
),
res_voos_aero_cia_aerea AS (
  SELECT 
    ICAO_aeroporto,
    ICAO_cia_area,
    voos_orig,
    voos_dest,
    (voos_orig + voos_dest) AS total_voos
  FROM ct_orig_dest
),
res_voos_aeroporto AS (
  SELECT 
    ICAO_aeroporto,
    SUM(voos_orig) AS sum_voos_orig,
    SUM(voos_dest) AS sum_voos_dest,
    SUM(total_voos) AS sum_total_voos
  FROM res_voos_aero_cia_aerea
  GROUP BY ICAO_aeroporto
),
top_1_aero_cia_area AS (
 SELECT
    ICAO_aeroporto,
    ICAO_cia_area,
    total_voos
FROM res_voos_aero_cia_aerea
QUALIFY RANK() OVER (PARTITION BY ICAO_aeroporto ORDER BY total_voos DESC) = 1
),
fav_aero_cia_area AS (
  SELECT
    t2.name AS nome_aeroporto,
    t1.ICAO_aeroporto,
    t3.razao_social AS razao_social_cia_area,
    t4.sum_voos_orig,
    t4.sum_voos_dest,
    t4.sum_total_voos
  FROM top_1_aero_cia_area AS t1
  LEFT JOIN `concise-crane-336515.eleflow.silver_icaos` AS t2
   ON t1.ICAO_aeroporto = t2.icao
  LEFT JOIN `concise-crane-336515.eleflow.silver_AIR_CIA` AS t3
    ON t1.ICAO_cia_area = t3.icao
  LEFT JOIN res_voos_aeroporto AS t4
    USING(ICAO_aeroporto)
)
SELECT * 
FROM fav_aero_cia_area
ORDER BY nome_aeroporto NULLS LAST
