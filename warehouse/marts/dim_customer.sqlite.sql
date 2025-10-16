-- SQLite dimension view mirroring staged customers.
DROP VIEW IF EXISTS dim_customer;
CREATE VIEW dim_customer AS
SELECT * FROM stg_customers;
