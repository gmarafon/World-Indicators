SELECT
  `Country` as `country`,
  `Economy` as `economy_id`,
  `Series` as `series`,
  `Series ID` as `series_id` ,
  `YR2014` as `2014`,
  `YR2015` as `2015`,
  `YR2016` as `2016`,
  `YR2017` as `2017`,
  `YR2018` as `2018`,
  `YR2019` as `2019`,
  `YR2020` as `2020`,
  `YR2021` as `2021`,
  `YR2022` as `2022`,
  `YR2023` as `2023`
FROM
  {{ source('world_indicators', 'world_bank_data') }}