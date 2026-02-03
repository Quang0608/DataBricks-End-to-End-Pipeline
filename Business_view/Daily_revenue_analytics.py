# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog ecommerce;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2) Daily revenue analytics view
# MAGIC CREATE OR REPLACE VIEW business.v_daily_revenue_analytics AS
# MAGIC WITH payments_by_order AS (
# MAGIC   SELECT
# MAGIC     order_id,
# MAGIC     SUM(CAST(payment_value AS DOUBLE)) AS paid_amount
# MAGIC   FROM silver.order_payments
# MAGIC   GROUP BY order_id
# MAGIC ),
# MAGIC orders_base AS (
# MAGIC   SELECT
# MAGIC     order_id,
# MAGIC     TO_DATE(order_purchase_ts) AS order_date,
# MAGIC     order_status
# MAGIC   FROM silver.orders
# MAGIC )
# MAGIC SELECT
# MAGIC   o.order_date                                    AS revenue_date,
# MAGIC   COUNT(DISTINCT o.order_id)                      AS orders_count,
# MAGIC   SUM(p.paid_amount)                              AS gross_revenue,
# MAGIC   AVG(p.paid_amount)                              AS avg_order_value
# MAGIC FROM orders_base o
# MAGIC JOIN payments_by_order p
# MAGIC   ON o.order_id = p.order_id
# MAGIC WHERE o.order_status IN ('delivered', 'shipped', 'invoiced', 'approved')
# MAGIC GROUP BY o.order_date;
# MAGIC
# MAGIC -- 3) Quick check
# MAGIC SELECT *
# MAGIC FROM business.v_daily_revenue_analytics
# MAGIC ORDER BY revenue_date;
# MAGIC