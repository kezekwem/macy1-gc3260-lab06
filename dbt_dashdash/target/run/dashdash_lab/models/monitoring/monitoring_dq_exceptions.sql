
  create view "neondb"."public_public"."monitoring_dq_exceptions__dbt_tmp"
    
    
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
        lower(trim(coalesce(status, ''))) as status_norm,
        row_number() over (partition by order_id order by order_timestamp) as rn
    from "neondb"."public"."orders"
),
dupes as (
    select order_id, 'duplicate_order' as reason
    from base
    where rn > 1
),
bad_rest_fk as (
    select o.order_id, 'bad_fk_restaurant' as reason
    from "neondb"."public"."orders" as o
    left join "neondb"."public_public"."stg_restaurants" as r on o.restaurant_id = r.restaurant_id
    where r.restaurant_id is null
),
bad_cour_fk as (
    select o.order_id, 'bad_fk_courier' as reason
    from "neondb"."public"."orders" as o
    left join "neondb"."public_public"."stg_couriers" as c on o.courier_id = c.courier_id
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
  );