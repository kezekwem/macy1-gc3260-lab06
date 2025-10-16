
  create view "neondb"."public_public"."stg_orders__dbt_tmp"
    
    
  as (
    

with base as (
    select
        order_id,
        customer_id,
        restaurant_id,
        courier_id,
        
    
        NULLIF(BTRIM(order_timestamp::text), '')::timestamp
    
 as order_ts,
        
    
        NULLIF(BTRIM(pickup_timestamp::text), '')::timestamp
    
 as pickup_ts,
        
    
        NULLIF(BTRIM(dropoff_timestamp::text), '')::timestamp
    
 as dropoff_ts,
        lower(nullif(trim(status), '')) as status_norm,
        payment_method,
        
    
        NULLIF(BTRIM(subtotal::text), '')::numeric(10, 2)
    
 as subtotal,
        
    
        NULLIF(BTRIM(delivery_fee::text), '')::numeric(10, 2)
    
 as delivery_fee,
        
    
        NULLIF(BTRIM(tip_amount::text), '')::numeric(10, 2)
    
 as tip_amount,
        
    
        NULLIF(BTRIM(distance_km::text), '')::numeric(6, 2)
    
 as distance_km,
        row_number() over (partition by order_id order by order_timestamp) as rn
    from "neondb"."public"."orders"
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
                then 
    
        EXTRACT(epoch FROM (dropoff_ts - pickup_ts)) / 60.0
    

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
        when status_final = 'delivered' and delivery_minutes <= 45 then 
    TRUE

        else 
    FALSE

    end as on_time_flag
from clean
  );