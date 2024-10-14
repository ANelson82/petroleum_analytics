{{ config(
  post_hook = "copy {{this}} to 's3://petroleum-data/output/novi_dead_letter_queue.csv'"
) }}

with src_data as (
select api10,
       direction,
       wellname,
       welltype,
       operator,
       basin,
       subbasin,
       state,
       county,
       spuddate,
       cum12moil,
       cum12mgas,
       cum12mwater,
from {{ ref('raw_data')}} )

select *
from src_data
where api10 is null