--The sum of each of cum12moil, cum12mgas and cum12mwater, by basin

select basin_cleansed as basin
     , sum(cum12moil_cleansed)   as sum_cum12moil
     , sum(cum12mgas_cleansed)   as sum_cum12mgas
     , sum(cum12mwater_cleansed) as sum_cum12mwater
from {{ ref('production_12mo_cums_cleansed') }}
group by basin_cleansed
order by 2 desc, 3 desc, 4 desc