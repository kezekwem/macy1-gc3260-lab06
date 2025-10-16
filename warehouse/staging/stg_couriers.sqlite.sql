-- SQLite-friendly version of the courier staging view.
-- Uses DROP VIEW IF EXISTS since SQLite lacks CREATE OR REPLACE VIEW.
DROP VIEW IF EXISTS stg_couriers;
CREATE VIEW stg_couriers AS
SELECT
  courier_id,
  courier_name,
  lower(vehicle_type) AS vehicle_type,
  active_from,
  active_to,
  region
FROM couriers;
