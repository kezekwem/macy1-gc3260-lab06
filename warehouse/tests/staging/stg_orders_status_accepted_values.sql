-- Domain test: verify the staged status field stays within the curated list.
SELECT *
FROM stg_orders
WHERE status NOT IN ('delivered','canceled','returned','unknown')
   OR status IS NULL;
