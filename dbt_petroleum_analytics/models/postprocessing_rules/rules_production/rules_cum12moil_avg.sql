select avg(cum12moil)::numeric(38,0) as cum12oil_avg
from {{ ref('fct_production_12mo_cums') }}