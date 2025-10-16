-- KPI view summarising delivery performance.
-- Highlights to teach:
--   • AVG with a CASE statement converts boolean flags into numeric proportions.
--   • FILTER clauses apply conditional counting without extra subqueries.
--   • NULLIF prevents division-by-zero by turning a zero denominator into NULL.
CREATE OR REPLACE VIEW kpi_delivery_overview AS
WITH d AS (SELECT * FROM fct_deliveries)
SELECT
  AVG(CASE WHEN on_time_flag THEN 1 ELSE 0 END)::numeric(5,4) AS on_time_rate,
  AVG(delivery_minutes)::numeric(6,2)                         AS avg_delivery_minutes,
  (
    SELECT
      (COUNT(*) FILTER (WHERE status IN ('canceled','returned'))::numeric
       / NULLIF(COUNT(*),0))
    FROM stg_orders
  )::numeric(5,4) AS cancel_return_rate
FROM d;
