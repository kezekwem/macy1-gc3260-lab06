
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select restaurant_id as from_field
    from "neondb"."public_public"."stg_orders"
    where restaurant_id is not null
),

parent as (
    select restaurant_id as to_field
    from "neondb"."public_public"."stg_restaurants"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test