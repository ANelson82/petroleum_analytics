select direction as direction_mostcommon
from {{ ref('dim_wells') }}
group by all
order by count(*) desc
limit 1