SELECT DISTINCT
  `country`,
  `economy_id`
FROM
  {{ ref('marts_fact_world_indicators') }}