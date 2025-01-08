{{ config(materialized='table') }}

SELECT DISTINCT
  `Country`,
  `Economy` as `Economy ID`
FROM
  `world-indicators-447017.dbt_world_indicators.src_world_indicators`