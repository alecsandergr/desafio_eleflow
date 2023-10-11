-- Databricks notebook source
DROP TABLE IF EXISTS silver.eleflow_icaos;

CREATE TABLE IF NOT EXISTS silver.eleflow_icaos AS

SELECT
  id,
  iata,
  icao,
  name as nome,
  location as localidade,
  IF(street_number = '', NULL, street_number) as numero_rua,
  IF(street = '', NULL, street) as rua,
  IF(city = '', NULL, city) as cidade,
  IF(county = '', NULL, county) as condado,
  IF(state = '', NULL, state) as estado,
  IF(country_iso = '', NULL, country_iso) as pais_iso,
  IF(country = '', NULL, country) as pais,
  IF(postal_code = '', NULL, postal_code) as codigo_postal,
  IF(phone = '', NULL, phone) as telefone,
  latitude,
  longitude,
  IF(website = '', NULL, website) as website,
  `timestamp`
FROM
  bronze.eleflow_icaos
;