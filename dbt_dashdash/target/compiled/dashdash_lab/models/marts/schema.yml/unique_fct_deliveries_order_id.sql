
    
    

select
    order_id as unique_field,
    count(*) as n_records

from "neondb"."public_public"."fct_deliveries"
where order_id is not null
group by order_id
having count(*) > 1


