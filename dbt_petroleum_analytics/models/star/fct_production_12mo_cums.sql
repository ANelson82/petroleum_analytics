select {{ dbt_utils.generate_surrogate_key(['api10', 'direction', 'wellname', 'welltype', 'spuddate']) }} as well_keyhash
     , {{ dbt_utils.generate_surrogate_key(['operator']) }} as operator_keyhash
     , {{ dbt_utils.generate_surrogate_key(['basin', 'subbasin', 'state', 'county']) }} as location_keyhash
     , try_cast(cum12moil as double) as cum12moil
     , try_cast(cum12mgas as double) as cum12mgas
     , try_cast(cum12mwater as double) as cum12mwater
     , 'novi_raw_data' as record_source
     , load_ts_utc
from {{ ref('stg_novi_data') }}
qualify row_number() over (partition by well_keyhash order by load_ts_utc) = 1