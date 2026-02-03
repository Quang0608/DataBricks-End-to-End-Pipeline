SELECT
  s.seller_id,
  ROUND(SUM(oi.price), 2) AS revenue
FROM gold.order_items oi
JOIN silver.sellers s
  ON oi.seller_id = s.seller_id
GROUP BY s.seller_id
ORDER BY revenue DESC
LIMIT 10;
