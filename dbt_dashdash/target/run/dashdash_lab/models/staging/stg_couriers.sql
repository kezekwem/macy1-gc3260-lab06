
  create view "neondb"."public_public"."stg_couriers__dbt_tmp"
    
    
  as (
    

select
  courier_id,
  courier_name,
  lower(vehicle_type) as vehicle_type,
  active_from,
  active_to,
  region
from "neondb"."public"."couriers"
  );