-- Numeric reasonableness test: delivery_minutes should be blank or zero/positive.
SELECT *
FROM stg_orders
WHERE NOT (delivery_minutes IS NULL OR delivery_minutes >= 0);
