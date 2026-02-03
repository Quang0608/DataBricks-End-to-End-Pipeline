CREATE OR REPLACE TABLE gold.dim_seller
USING DELTA
AS
WITH s AS (
  SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
  FROM silver.sellers
  WHERE IsActive = true
),
s_geo AS (
  SELECT
    s.*,
    g.geolocation_key
  FROM s
  LEFT JOIN gold.dim_geolocation g
    ON g.geolocation_zip_code_prefix = s.seller_zip_code_prefix
   AND lower(g.geolocation_city) = lower(s.seller_city)
   AND g.geolocation_state = s.seller_state
)
SELECT
  xxhash64(coalesce(seller_id, '')) AS seller_key,
  seller_id,
  seller_zip_code_prefix,
  seller_city,
  seller_state,
  geolocation_key
FROM s_geo;
