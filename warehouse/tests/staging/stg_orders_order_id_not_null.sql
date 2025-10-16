-- Data quality test: ensure every staged order carries a primary key.
SELECT * FROM stg_orders WHERE order_id IS NULL;
