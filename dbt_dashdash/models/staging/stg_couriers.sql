{{ config(materialized='view') }}

select
  courier_id,
  courier_name,
  lower(vehicle_type) as vehicle_type,
  active_from,
  active_to,
  region
from {{ source('dashdash', 'couriers') }}
