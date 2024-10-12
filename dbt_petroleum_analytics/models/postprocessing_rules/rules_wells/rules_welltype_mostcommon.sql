SELECT welltype as welltype_mostcommon
FROM {{ ref('dim_wells') }}
GROUP BY all
ORDER BY COUNT(*) DESC
LIMIT 1