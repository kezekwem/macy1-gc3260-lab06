-- Restaurant dimension view; a thin wrapper over staging so we can slot in transformations later.
CREATE OR REPLACE VIEW dim_restaurant AS
SELECT * FROM stg_restaurants;
