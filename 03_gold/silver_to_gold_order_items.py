CREATE OR REPLACE TABLE gold.fact_order_items
USING DELTA
AS
SELECT
  i.order_id,
  i.order_item_id,
  fo.customer_key,
  CAST(date_format(date(fo.order_purchase_ts), 'yyyyMMdd') AS INT) AS purchase_date_key,
  p.product_key,
  s.seller_key,
  CAST(date_format(date(i.shipping_limit_date), 'yyyyMMdd') AS INT) AS shipping_limit_date_key,
  i.shipping_limit_date,
  i.price,
  i.freight_value
FROM silver.order_items i
LEFT JOIN gold.fact_orders fo
  ON fo.order_id = i.order_id
LEFT JOIN gold.dim_product p
  ON p.product_id = i.product_id
LEFT JOIN gold.dim_seller s
  ON s.seller_id = i.seller_id;
