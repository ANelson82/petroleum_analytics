select basin as basin_bad_values
from {{ ref('stg_novi_data') }}
where regexp_matches(basin, '[0-9]')