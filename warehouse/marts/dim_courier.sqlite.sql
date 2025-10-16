-- SQLite dimension view for couriers with DROP to mimic OR REPLACE semantics.
DROP VIEW IF EXISTS dim_courier;
CREATE VIEW dim_courier AS
SELECT * FROM stg_couriers;
