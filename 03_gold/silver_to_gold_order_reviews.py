CREATE OR REPLACE TABLE gold.fact_reviews
USING DELTA
AS
SELECT
  r.review_id,
  r.order_id,
  fo.customer_key,
  CAST(date_format(date(fo.order_purchase_ts), 'yyyyMMdd') AS INT) AS purchase_date_key,
  CAST(date_format(date(r.review_creation_ts), 'yyyyMMdd') AS INT) AS review_creation_date_key,
  CAST(date_format(date(r.review_answer_ts), 'yyyyMMdd') AS INT) AS review_answer_date_key,
  r.review_score,
  r.review_comment_title,
  r.review_comment_message,
  r.review_creation_ts,
  r.review_answer_ts
FROM silver.order_reviews r
LEFT JOIN gold.fact_orders fo
  ON fo.order_id = r.order_id;
