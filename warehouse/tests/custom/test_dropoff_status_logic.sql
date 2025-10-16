-- Custom business rule: canceled/returned orders should not have a drop-off timestamp, while
-- delivered orders must record one. Any violations surface here for root-cause review.
SELECT *
FROM stg_orders
WHERE (status IN ('canceled','returned') AND dropoff_ts IS NOT NULL)
   OR (status = 'delivered' AND dropoff_ts IS NULL);
