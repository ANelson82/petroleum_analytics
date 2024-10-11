with production_12mo_cums as (
    select * from {{ ref('production_12mo_cums') }} 
)

--records with nulls:
-- welltype
-- operator_name
-- cum12mgas
-- cum12mwater


select api10
     , direction
     , wellname
     , welltype
     , spuddate
     , operator_name
     , basin
     , subbasin
     , state
     , county
     , cum12moil
     , cum12mgas
     , cum12mwater
     , production_load_ts
from production_12mo_cums