{{ config(materialized='view') }}

with d as (
    select *
    from {{ ref('fct_deliveries') }}
)
select
    {{ avg_on_time_expr('on_time_flag') }} as on_time_rate,
    {{ avg_delivery_minutes_expr('delivery_minutes') }} as avg_delivery_minutes,
    (
        select {{ cancel_return_rate_expr('status') }}
        from {{ ref('stg_orders') }}
    ) as cancel_return_rate
from d
