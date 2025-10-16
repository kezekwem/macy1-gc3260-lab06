-- Simple pass-through staging view; kept separate so future cleansing can be added without
-- touching downstream marts. SELECT * copies the raw customers table as-is for now.
CREATE OR REPLACE VIEW stg_customers AS
SELECT * FROM customers;
