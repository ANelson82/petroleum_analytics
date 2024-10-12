with epoch as (
SELECT avg(EXTRACT(epoch FROM spuddate)) as epoch_avg
FROM {{ ref('dim_wells') }} )
select to_timestamp(epoch_avg)::date as spuddate_avg from epoch