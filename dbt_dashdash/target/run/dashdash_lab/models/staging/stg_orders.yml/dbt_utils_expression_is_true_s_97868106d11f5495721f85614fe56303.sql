
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "neondb"."public_public"."stg_orders"

where not(delivery_minutes delivery_minutes IS NULL OR delivery_minutes >= 0)


  
  
      
    ) dbt_internal_test