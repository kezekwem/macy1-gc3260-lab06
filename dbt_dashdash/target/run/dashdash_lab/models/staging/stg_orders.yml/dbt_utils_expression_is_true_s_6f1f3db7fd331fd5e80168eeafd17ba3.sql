
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "neondb"."public_public"."stg_orders"

where not(dropoff_ts (status = 'delivered' AND dropoff_ts IS NOT NULL) OR (status IN ('canceled','returned','unknown') AND dropoff_ts IS NULL))


  
  
      
    ) dbt_internal_test