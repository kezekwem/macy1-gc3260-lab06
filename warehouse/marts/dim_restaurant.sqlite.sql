-- SQLite version of the restaurant dimension view.
DROP VIEW IF EXISTS dim_restaurant;
CREATE VIEW dim_restaurant AS
SELECT * FROM stg_restaurants;
