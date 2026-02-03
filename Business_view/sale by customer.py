# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog ecommerce;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS business;

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace view business.sales_by_customer as 
# MAGIC Select 
# MAGIC   date(o.order_purchase_ts) as purchase_date,
# MAGIC   o.customer_id,
# MAGIC   sum(oi.price) as total_price
# MAGIC From silver.order_items oi
# MAGIC Left Join silver.orders o 
# MAGIC   On oi.order_id = o.order_id
# MAGIC Group by 
# MAGIC   date(o.order_purchase_ts), o.customer_id, o.order_status