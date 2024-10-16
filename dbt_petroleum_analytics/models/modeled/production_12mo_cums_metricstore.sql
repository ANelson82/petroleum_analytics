--BRING IN DIMS AND FACTS
with dim_locations as (
    select * from {{ ref('dim_locations') }}
)

, dim_operators as (
    select * from {{ ref('dim_operators') }}
)

, dim_wells as (
    select * from {{ ref('dim_wells') }}
)

, fct_production_12mo_cums as (
    select * from {{ ref('fct_production_12mo_cums') }}
)

--BRING IN RULES

, rules_direction_mostcommon as (
    select * from {{ ref('rules_direction_mostcommon')}}
)

, rules_basin_mostcommon as (
    select * from {{ ref('rules_basin_mostcommon')}}
)

, rules_subbasin_mostcommon as (
    select * from {{ ref('rules_subbasin_mostcommon')}}
)

, rules_welltype_mostcommon as (
    select * from {{ ref('rules_welltype_mostcommon')}}
)

, rules_state_mostcommon as (
    select * from {{ ref('rules_state_mostcommon')}}
)

, rules_county_mostcommon as (
    select * from {{ ref('rules_county_mostcommon')}}
)

, rules_operator_name_mostcommon as (
    select * from {{ ref('rules_operator_name_mostcommon')}}
)

, rules_cum12mgas_avg as (
    select * from {{ ref('rules_cum12mgas_avg')}}
)

, rules_cum12moil_avg as (
    select * from {{ ref('rules_cum12moil_avg')}}
)

, rules_cum12mwater_avg as (
    select * from {{ ref('rules_cum12mwater_avg')}}
)

, rules_spuddate_avg as (
    select * from {{ ref('rules_spuddate_avg')}}
)

--records with nulls:
-- welltype
-- operator_name
-- cum12mgas
-- cum12mwater


, base_data as (
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
     , p.load_ts_utc as production_load_ts
from fct_production_12mo_cums p 
left join dim_wells w
  on p.well_keyhash = w.well_keyhash
left join dim_operators o 
  on p.operator_keyhash = o.operator_keyhash
left join dim_locations l 
  on p.location_keyhash = l.location_keyhash
)


select api10
     , direction
     , coalesce(direction, (select * from rules_direction_mostcommon)) as direction_cleansed
     , wellname
     , welltype
     , coalesce(welltype, (select * from rules_welltype_mostcommon)) as welltype_cleansed
     , spuddate
     , coalesce(spuddate, (select * from rules_spuddate_avg)) as spuddate_cleansed
     , operator_name
     , coalesce(operator_name, (select * from rules_operator_name_mostcommon)) as operator_name_cleansed
     , basin
     , coalesce(basin, (select * from rules_basin_mostcommon)) as basin_cleansed
     , subbasin
     , coalesce(subbasin, (select * from rules_subbasin_mostcommon)) as subbasin_cleansed
     , state
     , coalesce(state, (select * from rules_state_mostcommon)) as state_cleansed
     , county
     , coalesce(county, (select * from rules_county_mostcommon)) as county_cleansed
     , cum12moil
     , coalesce(cum12moil, (select * from rules_cum12moil_avg)) as cum12moil_cleansed
     , cum12mgas
     , coalesce(cum12mgas, (select * from rules_cum12mgas_avg)) as cum12mgas_cleansed
     , cum12mwater
     , coalesce(cum12mwater, (select * from rules_cum12mwater_avg)) as cum12mwater_cleansed
     , production_load_ts
from base_data
order by api10