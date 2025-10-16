-- Stage courier attributes and standardise text casing for lookups.
-- LOWER() keeps vehicle types consistent so equality checks do not break on mixed case input.
CREATE OR REPLACE VIEW stg_couriers AS
SELECT
  courier_id,
  courier_name,
  lower(vehicle_type) AS vehicle_type,
  active_from,
  active_to,
  region
FROM couriers;
