



select
    1
from "neondb"."public_public"."stg_orders"

where not(delivery_minutes delivery_minutes IS NULL OR delivery_minutes >= 0)

