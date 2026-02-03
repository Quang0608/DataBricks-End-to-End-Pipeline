CREATE OR REPLACE TABLE gold.dim_date
USING DELTA
AS
WITH bounds AS (
  SELECT
    date(min(order_purchase_ts)) AS min_d,
    date(max(order_purchase_ts)) AS max_d
  FROM silver.orders
),
dates AS (
  SELECT explode(sequence(min_d, max_d, interval 1 day)) AS d
  FROM bounds
)
SELECT
  CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
  d AS date_value,
  year(d) AS year,
  quarter(d) AS quarter,
  month(d) AS month,
  date_format(d, 'MMMM') AS month_name,
  day(d) AS day_of_month,
  dayofweek(d) AS day_of_week,
  date_format(d, 'EEEE') AS day_name,
  weekofyear(d) AS week_of_year,
  CASE WHEN dayofweek(d) IN (1,7) THEN true ELSE false END AS is_weekend
FROM dates;
