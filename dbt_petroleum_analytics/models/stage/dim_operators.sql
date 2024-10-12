select {{ dbt_utils.generate_surrogate_key(['operator']) }} as operator_keyhash
     , operator as operator_name
     , 'novi_raw_data' as record_source
     , load_ts
from {{ ref('raw_novi_data') }}
qualify row_number() over (partition by operator_keyhash order by load_ts) = 1