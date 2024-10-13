with basin_bad_values as (
     select * from {{ ref('basin_bad_values')}}
)

select {{ dbt_utils.generate_surrogate_key(['basin', 'subbasin', 'state', 'county']) }} as location_keyhash
     , case 
          when basin in (select basin_bad_values from basin_bad_values) then null
          else basin
       end as basin
     , subbasin
     , state
     , county
     , 'novi_raw_data' as record_source
     , load_ts_utc
from {{ ref('snsh_novi_data') }}
qualify row_number() over (partition by location_keyhash order by load_ts_utc) = 1