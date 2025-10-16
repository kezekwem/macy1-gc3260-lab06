-- Dimension domain test: surface couriers whose standardised vehicle_type falls outside the approved set.
SELECT *
FROM dim_courier
WHERE vehicle_type NOT IN ('bike','scooter','car') OR vehicle_type IS NULL;
