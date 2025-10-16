-- Fact-to-dimension integrity test: delivered orders must reference a known courier when the ID is present.
SELECT f.*
FROM fct_deliveries f
LEFT JOIN dim_courier d ON f.courier_id = d.courier_id
WHERE f.courier_id IS NOT NULL AND d.courier_id IS NULL;
