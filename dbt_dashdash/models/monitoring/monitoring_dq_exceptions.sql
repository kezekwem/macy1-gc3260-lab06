{{ config(materialized='view') }}

with base as (
    select
        order_id,
        customer_id,
        restaurant_id,
        courier_id,
        {{ safe_timestamp('order_timestamp') }} as order_ts,
        {{ safe_timestamp('pickup_timestamp') }} as pickup_ts,
        {{ safe_timestamp('dropoff_timestamp') }} as dropoff_ts,
        lower(trim(coalesce(status, ''))) as status_norm,
        row_number() over (partition by order_id order by order_timestamp) as rn
    from {{ source('dashdash', 'orders') }}
),
dupes as (
    select order_id, 'duplicate_order' as reason
    from base
    where rn > 1
),
bad_rest_fk as (
    select o.order_id, 'bad_fk_restaurant' as reason
    from {{ source('dashdash', 'orders') }} as o
    left join {{ ref('stg_restaurants') }} as r on o.restaurant_id = r.restaurant_id
    where r.restaurant_id is null
),
bad_cour_fk as (
    select o.order_id, 'bad_fk_courier' as reason
    from {{ source('dashdash', 'orders') }} as o
    left join {{ ref('stg_couriers') }} as c on o.courier_id = c.courier_id
    where o.courier_id is not null and c.courier_id is null
),
unknown_status as (
    select order_id, 'status_unknown' as reason
    from base
    where status_norm is null or status_norm not in ('delivered', 'canceled', 'returned')
)
select distinct order_id, reason
from (
    select * from dupes
    union all
    select * from bad_rest_fk
    union all
    select * from bad_cour_fk
    union all
    select * from unknown_status
) as unioned
order by order_id, reason
