
  create view "neondb"."public_public"."dim_restaurant__dbt_tmp"
    
    
  as (
    

select *
from "neondb"."public_public"."stg_restaurants"
  );