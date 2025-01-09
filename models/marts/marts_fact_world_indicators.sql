WITH unpivoted AS (
  SELECT 
    `Economy ID`,
    `Series ID`,
    `Series`,
    CAST(Year AS INT64) as Year, #to make sure the value is a number
    CAST(Value AS FLOAT64) as Value, #to make sure the value is a number
    REGEXP_EXTRACT(`Series`, r'.*\((.*?)\)') as `Measurement Unity`
  FROM 
    `world-indicators-447017.dbt_world_indicators.stg_world_indicators`
  UNPIVOT(
    Value FOR Year IN (
      `2014`, `2015`, `2016`, `2017`, `2018`, 
      `2019`, `2020`, `2021`, `2022`, `2023`
    )
  )
)

SELECT
  `Economy ID`,
  `Series ID`,
  `Series`,
  `Measurement Unity`,
  `Measurement Unity` as `Unity Type`,
  Year,
  Value
FROM 
  unpivoted
ORDER BY 
  `Economy ID`, 
  `Series ID`, 
  Year