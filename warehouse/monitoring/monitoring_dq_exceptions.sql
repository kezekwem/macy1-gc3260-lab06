-- Data quality monitor that surfaces orders needing manual review.
-- Demonstrates: CTE pipelines, NULLIF() to tidy empty strings, LOWER()+TRIM() for normalisation,
-- and UNION ALL to stitch together multiple exception reasons.
CREATE OR REPLACE VIEW monitoring_dq_exceptions AS
WITH base AS (
  SELECT
    order_id,
    customer_id,
    restaurant_id,
    courier_id,
    NULLIF(order_timestamp,'')::timestamp    AS order_ts,
    NULLIF(pickup_timestamp,'')::timestamp   AS pickup_ts,
    NULLIF(dropoff_timestamp,'')::timestamp  AS dropoff_ts,
    lower(trim(NULLIF(status,'')))           AS status_norm,
    row_number() OVER (PARTITION BY order_id ORDER BY order_timestamp) AS rn
  FROM orders
),
dupes AS (
  -- Any record beyond the first per order_id is flagged as a duplicate.
  SELECT order_id, 'duplicate_order' AS reason FROM base WHERE rn > 1
),
bad_rest_fk AS (
  -- LEFT JOIN + IS NULL pattern finds orders referencing missing restaurants.
  SELECT o.order_id, 'bad_fk_restaurant' AS reason
  FROM orders o
  LEFT JOIN stg_restaurants r ON o.restaurant_id = r.restaurant_id
  WHERE r.restaurant_id IS NULL
),
bad_cour_fk AS (
  -- Similar referential integrity check for couriers, ignoring NULL (no courier assigned).
  SELECT o.order_id, 'bad_fk_courier' AS reason
  FROM orders o
  LEFT JOIN stg_couriers c ON o.courier_id = c.courier_id
  WHERE o.courier_id IS NOT NULL AND c.courier_id IS NULL
),
unknown_status AS (
  -- Guard against unexpected status values slipping through the staging clean-up.
  SELECT order_id, 'status_unknown' AS reason
  FROM base
  WHERE status_norm IS NULL OR status_norm NOT IN ('delivered','canceled','returned')
)
SELECT DISTINCT order_id, reason
FROM (
  SELECT * FROM dupes
  UNION ALL SELECT * FROM bad_rest_fk
  UNION ALL SELECT * FROM bad_cour_fk
  UNION ALL SELECT * FROM unknown_status
) u
ORDER BY order_id, reason;
