
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        vehicle_type as value_field,
        count(*) as n_records

    from "neondb"."public_public"."stg_couriers"
    group by vehicle_type

)

select *
from all_values
where value_field not in (
    'bike','scooter','car'
)



  
  
      
    ) dbt_internal_test