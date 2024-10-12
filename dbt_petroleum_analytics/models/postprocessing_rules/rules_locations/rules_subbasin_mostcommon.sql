select subbasin as subbasin_mostcommon
from {{ ref('dim_locations') }}
group by all
order by count(*) desc
limit 1