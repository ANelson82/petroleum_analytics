with basin_bad_values as (
     select * from {{ ref('basin_bad_values')}}
)

, subbasin_bad_values as (
     select * from {{ ref('subbasin_bad_values')}}
)

select {{ dbt_utils.generate_surrogate_key(['basin', 'subbasin', 'state', 'county']) }} as location_keyhash
     , case 
          when basin in (select basin_bad_values from basin_bad_values) then null
          else basin
       end as basin
     , case 
          when subbasin in (select subbasin_bad_values from subbasin_bad_values) then null
          else subbasin
       end as subbasin
     , state
     , county
     , 'novi_raw_data' as record_source
     , load_ts_utc
from {{ ref('stg_novi_data') }}
qualify row_number() over (partition by location_keyhash order by load_ts_utc) = 1