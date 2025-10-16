-- SQLite variant of the customers staging view with a defensive DROP.
DROP VIEW IF EXISTS stg_customers;
CREATE VIEW stg_customers AS
SELECT * FROM customers;
