
  create view "neondb"."public_public"."fct_deliveries__dbt_tmp"
    
    
  as (
    

select *
from "neondb"."public_public"."stg_orders" as o
where o.status = 'delivered'
  and o.restaurant_id in (select restaurant_id from "neondb"."public_public"."stg_restaurants")
  and (
        o.courier_id is null
        or o.courier_id in (select courier_id from "neondb"."public_public"."stg_couriers")
      )
  );