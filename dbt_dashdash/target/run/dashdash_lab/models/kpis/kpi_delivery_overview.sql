
  create view "neondb"."public_public"."kpi_delivery_overview__dbt_tmp"
    
    
  as (
    

with d as (
    select *
    from "neondb"."public_public"."fct_deliveries"
)
select
    
    
        AVG(CASE WHEN on_time_flag THEN 1 ELSE 0 END)::numeric(5, 4)
    
 as on_time_rate,
    
    
        AVG(delivery_minutes)::numeric(6, 2)
    
 as avg_delivery_minutes,
    (
        select 
    
        (COUNT(*) FILTER (WHERE status IN ('canceled', 'returned'))::numeric / NULLIF(COUNT(*), 0))
    

        from "neondb"."public_public"."stg_orders"
    ) as cancel_return_rate
from d
  );