select api10,
       direction,
       wellname,
       welltype,
       spuddate,
       operator_name,
       basin,
       subbasin,
       state,
       county,
       cum12moil,
       cum12mgas,
       cum12mwater,
       production_load_ts
from {{ ref('production_12mo_cums_metricstore')}}