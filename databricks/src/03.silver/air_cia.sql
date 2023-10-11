-- Databricks notebook source
DROP TABLE IF EXISTS silver.eleflow_air_cia;

CREATE TABLE IF NOT EXISTS silver.eleflow_air_cia AS

SELECT
  razao_social,
  SPLIT_PART(icao_iata, ' ', 1) AS icao,
  IF(SPLIT_PART(icao_iata, ' ', 2) = '', null, SPLIT_PART(icao_iata, ' ', 2)) AS iata,
  cnpj,
  atividades_aereas,
  endereco_sede,
  telefone,
  TRIM(email) AS email,
  decisao_operacional,
  TO_DATE(data_decisao_operacional, 'dd/MM/yyyy') AS data_decisao_operacional,
  TO_DATE(validade_operacional, 'dd/MM/yyyy') AS validade_operacional,
  `timestamp`
FROM
  bronze.eleflow_air_cia
;