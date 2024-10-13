select basin as basin_bad_values
from {{ ref('snsh_novi_data') }}
where regexp_matches(basin, '[0-9]')