{{ config(materialized='table') }}

SELECT DISTINCT
  `Series`,
  `Series ID`
FROM
  `world-indicators-447017.dbt_world_indicators.src_world_indicators`