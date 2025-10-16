
  create view "neondb"."public_public"."stg_restaurants__dbt_tmp"
    
    
  as (
    

select
  *
from "neondb"."public"."restaurants"
  );