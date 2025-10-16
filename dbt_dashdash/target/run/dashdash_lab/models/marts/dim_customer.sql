
  create view "neondb"."public_public"."dim_customer__dbt_tmp"
    
    
  as (
    

select *
from "neondb"."public_public"."stg_customers"
  );