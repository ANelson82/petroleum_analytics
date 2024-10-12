select basin as basin_mostcommon
from {{ ref('dim_locations') }}
group by all
order by count(*) desc
limit 1