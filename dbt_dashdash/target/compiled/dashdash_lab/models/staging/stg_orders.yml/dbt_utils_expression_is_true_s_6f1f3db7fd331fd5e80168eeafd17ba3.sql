



select
    1
from "neondb"."public_public"."stg_orders"

where not(dropoff_ts (status = 'delivered' AND dropoff_ts IS NOT NULL) OR (status IN ('canceled','returned','unknown') AND dropoff_ts IS NULL))

