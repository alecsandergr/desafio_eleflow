--  Criar AS seguintes views (Priorize o uso de SQL para esta parte):
--    Para cada companhia aérea trazer a rota mais utilizada com AS seguintes informações:
--     - Razão social da companhia aérea
--     - Nome Aeroporto de Origem
--     - ICAO do aeroporto de origem
--     - Estado/UF do aeroporto de origem
--     - Nome do Aeroporto de Destino
--     - ICAO do Aeroporto de destino
--     - Estado/UF do aeroporto de destino

CREATE OR REPLACE VIEW `concise-crane-336515.eleflow.fav_rota_cia_area` AS

WITH rotas_por_cia_area AS (
  SELECT
    ICAO_empresa_area,
    ICAO_aerodromo_origem,
    ICAO_aerodromo_destino,
    COUNT(*) AS cont_rot_cia_area
  FROM `concise-crane-336515.eleflow.silver_VRA`
  GROUP BY 1,2,3
),
top_1_rota_cia_area AS (
  SELECT
    ICAO_empresa_area,
    ICAO_aerodromo_origem,
    ICAO_aerodromo_destino,
    rotas_por_cia_area.cont_rot_cia_area
  FROM rotas_por_cia_area
  QUALIFY RANK() OVER(PARTITION BY ICAO_empresa_area ORDER BY rotas_por_cia_area.cont_rot_cia_area DESC) = 1
)
SELECT 
  t2.razao_social AS razao_social_cia_area,
  t3.name AS nome_aeroporto_origem,
  t1.ICAO_aerodromo_origem AS ICAO_aeroporto_origem,
  t3.state AS estado_origem,
  t4.name AS nome_aeroporto_destino,
  t1.ICAO_aerodromo_destino AS ICAO_aeroporto_destino,
  t4.state AS estado_destino
FROM top_1_rota_cia_area AS t1
LEFT JOIN `concise-crane-336515.eleflow.silver_AIR_CIA` AS t2
  ON t1.ICAO_empresa_area = t2.icao
LEFT JOIN `concise-crane-336515.eleflow.silver_icaos` AS t3
  ON t1.ICAO_aerodromo_origem = t3.icao
LEFT JOIN `concise-crane-336515.eleflow.silver_icaos` AS t4
  ON t1.ICAO_aerodromo_destino = t4.icao