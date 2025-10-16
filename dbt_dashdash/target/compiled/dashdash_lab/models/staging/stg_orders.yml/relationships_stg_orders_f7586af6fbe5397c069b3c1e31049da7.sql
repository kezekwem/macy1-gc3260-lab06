
    
    

with child as (
    select courier_id as from_field
    from (select * from "neondb"."public_public"."stg_orders" where courier_id IS NOT NULL) dbt_subquery
    where courier_id is not null
),

parent as (
    select courier_id as to_field
    from "neondb"."public_public"."stg_couriers"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


