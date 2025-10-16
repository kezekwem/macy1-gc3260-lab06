{{ config(materialized='view') }}

select *
from {{ ref('stg_orders') }} as o
where o.status = 'delivered'
  and o.restaurant_id in (select restaurant_id from {{ ref('stg_restaurants') }})
  and (
        o.courier_id is null
        or o.courier_id in (select courier_id from {{ ref('stg_couriers') }})
      )
