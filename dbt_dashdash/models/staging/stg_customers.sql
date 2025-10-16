{{ config(materialized='view') }}

select
  *
from {{ source('dashdash', 'customers') }}
