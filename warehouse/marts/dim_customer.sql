-- Customer dimension sourced from the staging layer. Keeping this view separate means we can
-- add surrogate keys or slowly changing logic later without breaking downstream queries.
CREATE OR REPLACE VIEW dim_customer AS
SELECT * FROM stg_customers;
