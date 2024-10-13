with src_data as (
select api10,
       direction,
       wellname,
       welltype,
       operator,
       basin,
       subbasin,
       state,
       county,
       spuddate,
       cum12moil,
       cum12mgas,
       cum12mwater,
       load_ts
from {{ ref('raw_novi_data')}} )

, hashed as (
    select
          {{ dbt_utils.generate_surrogate_key(['api10']) }} as api10_hkey
        , {{ dbt_utils.generate_surrogate_key([ 'direction', 'wellname',
                'welltype', 'operator', 'basin',
                'subbasin', 'state', 'county', 'spuddate',
                'cum12moil', 'cum12mgas', 'cum12mwater'
                ]) }} as novi_data_dhiff
        , *
        , '{{ run_started_at }}'::timestamp as load_ts_utc
    from src_data
)

select * from hashed