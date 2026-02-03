SELECT
  DATE(order_purchase_timestamp) AS signup_date,
  COUNT(DISTINCT customer_unique_id) AS new_customers
FROM gold.orders o
JOIN silver.customers c
  ON o.customer_id = c.customer_id
GROUP BY DATE(order_purchase_timestamp)
ORDER BY signup_date;
