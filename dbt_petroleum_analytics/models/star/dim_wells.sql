select {{ dbt_utils.generate_surrogate_key(['api10', 'direction', 'wellname', 'welltype', 'spuddate']) }} as well_keyhash
     , api10
     , direction
     , wellname
     , welltype
     , try_cast(spuddate as date) as spuddate
     , 'novi_raw_data' as record_source
     , load_ts
from {{ ref('snsh_novi_data') }}
qualify row_number() over (partition by well_keyhash order by load_ts) = 1