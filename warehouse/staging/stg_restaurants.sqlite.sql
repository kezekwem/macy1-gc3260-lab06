-- SQLite variant of the restaurants staging view.
DROP VIEW IF EXISTS stg_restaurants;
CREATE VIEW stg_restaurants AS
SELECT * FROM restaurants;
