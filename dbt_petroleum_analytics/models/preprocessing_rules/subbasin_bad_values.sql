select subbasin as subbasin_bad_values
from {{ ref('stg_novi_data') }}
where regexp_matches(subbasin, '[0-9]')