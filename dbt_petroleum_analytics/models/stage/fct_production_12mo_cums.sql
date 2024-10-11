select {{ dbt_utils.generate_surrogate_key(['api10', 'direction', 'wellname', 'welltype', 'spuddate']) }} as well_keyhash
     , {{ dbt_utils.generate_surrogate_key(['operator']) }} as operator_keyhash
     , {{ dbt_utils.generate_surrogate_key(['basin', 'subbasin', 'state', 'county']) }} as location_keyhash
     , cum12moil
     , cum12mgas
     , cum12mwater
     , 'novi_csv' as record_source
     , load_ts
from {{ ref('raw_data') }}
qualify row_number() over (partition by well_keyhash order by load_ts) = 1