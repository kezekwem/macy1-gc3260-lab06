-- Uniqueness test: each delivered order should appear exactly once in the fact view.
SELECT order_id, COUNT(*) AS cnt
FROM fct_deliveries
GROUP BY order_id
HAVING COUNT(*) <> 1;
