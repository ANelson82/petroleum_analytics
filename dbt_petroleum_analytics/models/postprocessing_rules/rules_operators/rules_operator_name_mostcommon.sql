select operator_name as operator_name_mostcommon
from {{ ref('dim_operators') }}
group by all
order by count(*) desc
limit 1