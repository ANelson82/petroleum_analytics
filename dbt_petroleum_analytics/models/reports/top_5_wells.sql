{{ config(
  post_hook = "copy {{this}} to 's3://petroleum-data/output/top_5_wells.csv'"
) }}

--The top 5 oil wells by cum12moil, sorted in descending order

select api10
     , operator_name_cleansed as operator_name
     , wellname
     , state_cleansed as state
     , county_cleansed as county
     , cum12moil_cleansed
from {{ ref('production_12mo_cums_cleansed')}}
order by cum12moil_cleansed desc
limit 5