{{ config(
  post_hook = "copy {{this}} to 's3://petroleum-data/output/metric_store.csv'"
) }}

select *
from {{ ref('production_12mo_cums_metricstore')}}