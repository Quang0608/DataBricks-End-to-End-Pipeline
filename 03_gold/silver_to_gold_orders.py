CREATE OR REPLACE TABLE gold.fact_orders
USING DELTA
AS
SELECT
  o.order_id,
  c.customer_key,
  CAST(date_format(date(o.order_purchase_ts), 'yyyyMMdd') AS INT) AS purchase_date_key,
  CAST(date_format(date(o.order_approved_ts), 'yyyyMMdd') AS INT) AS approved_date_key,
  CAST(date_format(date(o.order_delivered_carrier_ts), 'yyyyMMdd') AS INT) AS delivered_carrier_date_key,
  CAST(date_format(date(o.order_delivered_customer_ts), 'yyyyMMdd') AS INT) AS delivered_customer_date_key,
  CAST(date_format(date(o.order_estimated_delivery_ts), 'yyyyMMdd') AS INT) AS estimated_delivery_date_key,
  o.order_status,
  o.order_purchase_ts,
  o.order_approved_ts,
  o.order_delivered_carrier_ts,
  o.order_delivered_customer_ts,
  o.order_estimated_delivery_ts
FROM silver.orders o
LEFT JOIN gold.dim_customer c
  ON c.customer_id = o.customer_id;
