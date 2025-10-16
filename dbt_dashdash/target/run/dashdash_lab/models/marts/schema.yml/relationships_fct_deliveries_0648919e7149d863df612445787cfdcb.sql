
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select courier_id as from_field
    from (select * from "neondb"."public_public"."fct_deliveries" where courier_id IS NOT NULL) dbt_subquery
    where courier_id is not null
),

parent as (
    select courier_id as to_field
    from "neondb"."public_public"."dim_courier"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test