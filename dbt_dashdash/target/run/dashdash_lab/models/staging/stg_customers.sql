
  create view "neondb"."public_public"."stg_customers__dbt_tmp"
    
    
  as (
    

select
  *
from "neondb"."public"."customers"
  );