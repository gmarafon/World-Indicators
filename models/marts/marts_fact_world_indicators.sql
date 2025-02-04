WITH unpivoted AS (
  SELECT 
    `economy_id`,
    `series_id`,
    `series`,
    `country`,
    CAST(Year AS INT64) as year, --to make sure the value is a number
    CAST(Value AS FLOAT64) as value, --to make sure the value is a number
    REGEXP_EXTRACT(`series`, r'.*\((.*?)\)') as `measurement_unity`
  FROM 
    {{ ref('stg_world_indicators') }}
  UNPIVOT(
    Value FOR Year IN (
      `2014`, `2015`, `2016`, `2017`, `2018`, 
      `2019`, `2020`, `2021`, `2022`, `2023`
    )
  )
)

SELECT
  `economy_id`,
  `series_id`,
  `series`,
  `country`,

  -- Extracting units of measurement from indicators and classifying it
  case
    WHEN REGEXP_CONTAINS(COALESCE(measurement_unity, series), r'per ') THEN 'Ratio'
    WHEN REGEXP_CONTAINS(COALESCE(measurement_unity, series), r'(1=)|(GPI)|(= 100)|(index)') THEN 'Index'
    WHEN REGEXP_CONTAINS(COALESCE(measurement_unity, series), r'(1 in)|(/)|(ratio)') THEN 'Ratio'
    WHEN REGEXP_CONTAINS(COALESCE(measurement_unity, series), r'(scale)|( to )') THEN 'Scale'
    WHEN REGEXP_CONTAINS(series, r'(%)|(Percentile)') THEN 'Percentual'
    WHEN REGEXP_CONTAINS(COALESCE(measurement_unity, series), r'(\$)|(USD)|(GNI)|(LCU)') THEN 'Monetary'
    ELSE 'Quantity' END as `unity_category`,
    
  year,
  value
FROM 
  unpivoted
ORDER BY 
  `economy_id`, 
  `series_id`, 
  year