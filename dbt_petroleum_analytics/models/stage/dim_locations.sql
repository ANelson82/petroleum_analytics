select {{ dbt_utils.generate_surrogate_key(['basin', 'subbasin', 'state', 'county']) }} as location_keyhash
     , basin
     , subbasin
     , state
     , county
     , load_ts
from {{ ref('raw_data') }}
qualify row_number() over (partition by location_keyhash order by load_ts) = 1