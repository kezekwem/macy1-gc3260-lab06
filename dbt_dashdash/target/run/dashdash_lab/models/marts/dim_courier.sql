
  create view "neondb"."public_public"."dim_courier__dbt_tmp"
    
    
  as (
    

select *
from "neondb"."public_public"."stg_couriers"
  );