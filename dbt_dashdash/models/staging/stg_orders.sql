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
        lower(nullif(trim(status), '')) as status_norm,
        payment_method,
        {{ safe_numeric('subtotal', 10, 2) }} as subtotal,
        {{ safe_numeric('delivery_fee', 10, 2) }} as delivery_fee,
        {{ safe_numeric('tip_amount', 10, 2) }} as tip_amount,
        {{ safe_numeric('distance_km', 6, 2) }} as distance_km,
        row_number() over (partition by order_id order by order_timestamp) as rn
    from {{ source('dashdash', 'orders') }}
),
dedup as (
    select *
    from base
    where rn = 1
),
clean as (
    select
        *,
        case
            when status_norm in ('delivered', 'canceled', 'returned') then status_norm
            when status_norm is null or status_norm = '' then 'unknown'
            else 'unknown'
        end as status_final,
        case
            when pickup_ts is not null and dropoff_ts is not null
                then {{ delivery_minutes_expr('dropoff_ts', 'pickup_ts') }}
            else null
        end as delivery_minutes
    from dedup
)
select
    order_id,
    customer_id,
    restaurant_id,
    courier_id,
    order_ts,
    pickup_ts,
    dropoff_ts,
    status_final as status,
    payment_method,
    subtotal,
    delivery_fee,
    tip_amount,
    distance_km,
    delivery_minutes,
    case
        when status_final = 'delivered' and delivery_minutes <= 45 then {{ bool_true() }}
        else {{ bool_false() }}
    end as on_time_flag
from clean
