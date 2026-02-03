CREATE OR REPLACE TABLE gold.dim_geolocation
USING DELTA
AS
SELECT
  xxhash64(
    coalesce(cast(geolocation_zip_code_prefix as string), ''),
    coalesce(cast(geolocation_city as string), ''),
    coalesce(cast(geolocation_state as string), ''),
    coalesce(cast(geolocation_lat as string), ''),
    coalesce(cast(geolocation_lng as string), '')
  ) AS geolocation_key,
  geolocation_zip_code_prefix,
  geolocation_city,
  geolocation_state,
  geolocation_lat,
  geolocation_lng
FROM silver.geolocation
WHERE geolocation_zip_code_prefix IS NOT NULL
GROUP BY
  geolocation_zip_code_prefix,
  geolocation_city,
  geolocation_state,
  geolocation_lat,
  geolocation_lng;
