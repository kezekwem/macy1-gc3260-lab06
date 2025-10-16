-- Dimension view that keeps courier attributes in a slowly-changing friendly layout.
-- Pulling from staging ensures any cleansing applied upstream flows through consistently.
CREATE OR REPLACE VIEW dim_courier AS
SELECT * FROM stg_couriers;
