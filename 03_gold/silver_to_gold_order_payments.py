CREATE OR REPLACE TABLE gold.fact_payments
USING DELTA
AS
SELECT
  p.order_id,
  p.payment_sequential,
  fo.customer_key,
  CAST(date_format(date(fo.order_purchase_ts), 'yyyyMMdd') AS INT) AS purchase_date_key,
  p.payment_type,
  p.payment_installments,
  p.payment_value
FROM silver.order_payments p
LEFT JOIN gold.fact_orders fo
  ON fo.order_id = p.order_id;
