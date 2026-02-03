CREATE OR REPLACE TABLE gold.dim_product
USING DELTA
AS
WITH p AS (
  SELECT
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
  FROM silver.products
  WHERE IsActive = true
),
t AS (
  SELECT
    product_category_name,
    product_category_name_english
  FROM silver.product_category_translation
)
SELECT
  xxhash64(coalesce(p.product_id, '')) AS product_key,
  p.product_id,
  p.product_category_name,
  coalesce(t.product_category_name_english, p.product_category_name) AS product_category_name_english,
  p.product_name_lenght,
  p.product_description_lenght,
  p.product_photos_qty,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm
FROM p
LEFT JOIN t
  ON t.product_category_name = p.product_category_name;
