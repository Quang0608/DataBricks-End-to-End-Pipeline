SELECT
  p.product_category_name,
  ROUND(SUM(oi.price), 2) AS revenue
FROM gold.order_items oi
JOIN silver.products p
  ON oi.product_id = p.product_id
GROUP BY p.product_category_name
ORDER BY revenue DESC
LIMIT 10;
