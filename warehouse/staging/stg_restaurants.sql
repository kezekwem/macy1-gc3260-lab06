-- Mirror the restaurants source into a staging layer, leaving room for future business rules.
CREATE OR REPLACE VIEW stg_restaurants AS
SELECT * FROM restaurants;
