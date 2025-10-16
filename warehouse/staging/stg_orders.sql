-- Purpose: cleanse and standardize order records before they feed marts and KPIs.
-- Key techniques demonstrated:
--   • Common Table Expressions (WITH) break the transformation into named steps.
--   • ROW_NUMBER window function keeps the first version of any duplicate order_id.
--   • ::timestamp and ::numeric casts normalize text CSV values into typed columns.
--   • CASE expressions compute derived fields such as status buckets and SLA flags.
CREATE OR REPLACE VIEW stg_orders AS
WITH base AS (
  -- Trim whitespace and cast each raw CSV column into strong types for downstream joins.
  SELECT
    order_id,
    customer_id,
    restaurant_id,
    courier_id,
    NULLIF(BTRIM(order_timestamp::text),'')::timestamp    AS order_ts,
    NULLIF(BTRIM(pickup_timestamp::text),'')::timestamp   AS pickup_ts,
    NULLIF(BTRIM(dropoff_timestamp::text),'')::timestamp  AS dropoff_ts,
    lower(NULLIF(BTRIM(status::text),''))                 AS status_norm,
    payment_method,
    NULLIF(BTRIM(subtotal::text),'')::numeric(10,2)       AS subtotal,
    NULLIF(BTRIM(delivery_fee::text),'')::numeric(10,2)   AS delivery_fee,
    NULLIF(BTRIM(tip_amount::text),'')::numeric(10,2)     AS tip_amount,
    NULLIF(BTRIM(distance_km::text),'')::numeric(6,2)     AS distance_km,
    row_number() OVER (PARTITION BY order_id ORDER BY order_timestamp) AS rn
  FROM orders
),
dedup AS (
  -- Keep only the first chronological record per order to eliminate duplicates.
  SELECT * FROM base WHERE rn = 1
),
clean AS (
  SELECT
    *,
    -- CASE categorises the free-form status values into an accepted list to simplify checks.
    CASE
      WHEN status_norm IN ('delivered','canceled','returned') THEN status_norm
      WHEN status_norm IS NULL THEN 'unknown'
      ELSE 'unknown'
    END AS status_final,
    -- EXTRACT(EPOCH ...) converts an interval into seconds so we can derive minutes in numeric form.
    CASE
      WHEN pickup_ts IS NOT NULL AND dropoff_ts IS NOT NULL
        THEN EXTRACT(epoch FROM (dropoff_ts - pickup_ts))/60.0
      ELSE NULL
    END AS delivery_minutes
  FROM dedup
)
SELECT
  order_id,
  customer_id,
  restaurant_id,
  courier_id,
  order_ts,
  pickup_ts,
  dropoff_ts,
  status_final AS status,
  payment_method,
  subtotal,
  delivery_fee,
  tip_amount,
  distance_km,
  delivery_minutes,
  -- SLA flag: mark true only when a delivered order finished within 45 minutes.
  CASE WHEN status_final = 'delivered' AND delivery_minutes <= 45 THEN TRUE ELSE FALSE END AS on_time_flag
FROM clean;
