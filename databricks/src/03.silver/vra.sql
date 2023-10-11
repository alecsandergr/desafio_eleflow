-- Databricks notebook source
DROP TABLE IF EXISTS silver.eleflow_vra;

CREATE TABLE IF NOT EXISTS silver.eleflow_vra AS 

SELECT
  TO_TIMESTAMP(chegada_prevista) AS chegada_prevista,
  TO_TIMESTAMP(chegada_real) AS chegada_real,
  nullif(codigo_justificativa, 'N/A') AS codigo_justificativa,
  codigo_tipo_linha,
  ICAO_aerodromo_destino,
  ICAO_aerodromo_origem,
  ICAO_empresa_area,
  numero_voo,
  TO_TIMESTAMP(partida_prevista) AS partida_prevista,
  TO_TIMESTAMP(partida_real) AS partida_real,
  situacao_voo,
  `timestamp`
FROM bronze.eleflow_vra
;