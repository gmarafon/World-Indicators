SELECT DISTINCT
  `series`,
  `series_id`
FROM
  {{ ref('marts_fact_world_indicators') }}