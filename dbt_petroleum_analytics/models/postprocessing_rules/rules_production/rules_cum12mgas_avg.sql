select avg(cum12mgas)::numeric(38,0) as cum12mgas_avg
from {{ ref('fct_production_12mo_cums') }}