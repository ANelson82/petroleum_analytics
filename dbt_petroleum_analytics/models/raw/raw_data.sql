select *
from {{ source('external_source', 'source1') }} 