select {{ dbt_utils.generate_surrogate_key(['operator']) }} as operator_keyhash
     , operator as operator_name
     , load_ts::date as meta_valid_from
     , '9999-12-31' as meta_valid_to
from {{ ref('raw_data') }}
qualify row_number() over (partition by operator_keyhash order by load_ts) = 1