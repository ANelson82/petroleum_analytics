
{{ config(
    materialized="view"
) }}


with fct_production_12mo_cums as (
    select * from {{ ref('fct_production_12mo_cums') }} 
)

, dim_locations as (
    select * from {{ ref('dim_locations') }}
)

, dim_operators as (
    select * from {{ ref('dim_operators') }}
)

, dim_wells as (
    select * from {{ ref('dim_wells') }}
)

select w.api10
     , w.direction
     , w.wellname
     , w.welltype
     , w.spuddate
     , o.operator_name
     , l.basin
     , l.subbasin
     , l.state
     , l.county
     , p.cum12moil
     , p.cum12mgas
     , p.cum12mwater
     , p.load_ts as production_load_ts
from fct_production_12mo_cums p 
left join dim_wells w
  on p.well_keyhash = w.well_keyhash
left join dim_operators o 
  on p.operator_keyhash = o.operator_keyhash
left join dim_locations l 
  on p.location_keyhash = l.location_keyhash
order by 1