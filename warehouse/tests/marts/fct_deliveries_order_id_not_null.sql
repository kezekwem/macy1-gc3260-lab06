-- Ensure the fact view retains a primary key for each delivery record.
SELECT * FROM fct_deliveries WHERE order_id IS NULL;
