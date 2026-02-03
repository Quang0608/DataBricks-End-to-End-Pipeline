CREATE OR REPLACE TABLE gold.dim_customer
USING DELTA
AS
WITH c AS (
  SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
  FROM silver.customers
  WHERE IsActive = true
),
c_geo AS (
  SELECT
    c.*,
    g.geolocation_key
  FROM c
  LEFT JOIN gold.dim_geolocation g
    ON g.geolocation_zip_code_prefix = c.customer_zip_code_prefix
   AND lower(g.geolocation_city) = lower(c.customer_city)
   AND g.geolocation_state = c.customer_state
)
SELECT
  xxhash64(coalesce(customer_id, '')) AS customer_key,
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state,
  geolocation_key
FROM c_geo;
