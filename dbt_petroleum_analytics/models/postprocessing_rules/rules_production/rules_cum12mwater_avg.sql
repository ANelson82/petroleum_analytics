select avg(cum12mwater)::numeric(38,0) as cum12mwater_avg
from {{ ref('fct_production_12mo_cums') }}