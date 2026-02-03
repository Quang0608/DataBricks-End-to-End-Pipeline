SELECT
  DATE_TRUNC('month', order_purchase_timestamp) AS month,
  ROUND(SUM(payment_value), 2) AS revenue
FROM gold.orders o
JOIN gold.order_payments p
  ON o.order_id = p.order_id
GROUP BY DATE_TRUNC('month', order_purchase_timestamp)
ORDER BY month;
