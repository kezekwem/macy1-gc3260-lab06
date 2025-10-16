{{ config(materialized='view') }}

select
  *
from {{ source('dashdash', 'restaurants') }}
