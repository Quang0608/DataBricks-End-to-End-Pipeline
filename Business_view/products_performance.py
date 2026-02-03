# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog ecommerce

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC create or replace view business.product_performance as
# MAGIC with orders as (
# MAGIC   select
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     order_status,
# MAGIC     cast(order_purchase_ts as timestamp) as order_purchase_ts,
# MAGIC     cast(order_delivered_customer_ts as timestamp) as delivered_customer_ts
# MAGIC   from silver.orders
# MAGIC ),
# MAGIC items as (
# MAGIC   select
# MAGIC     order_id,
# MAGIC     product_id,
# MAGIC     seller_id,
# MAGIC     cast(price as double) as item_price,
# MAGIC     cast(freight_value as double) as freight_value,
# MAGIC     cast(shipping_limit_date as timestamp) as shipping_limit_ts
# MAGIC   from silver.order_items
# MAGIC ),
# MAGIC payments as (
# MAGIC   select
# MAGIC     order_id,
# MAGIC     sum(cast(payment_value as double)) as paid_value
# MAGIC   from silver.order_payments
# MAGIC   group by order_id
# MAGIC ),
# MAGIC reviews as (
# MAGIC   select
# MAGIC     order_id,
# MAGIC     avg(cast(review_score as double)) as avg_review_score
# MAGIC   from silver.order_reviews
# MAGIC   group by order_id
# MAGIC ),
# MAGIC products as (
# MAGIC   select
# MAGIC     product_id,
# MAGIC     product_category_name
# MAGIC   from silver.products
# MAGIC ),
# MAGIC cat as (
# MAGIC   select
# MAGIC     product_category_name,
# MAGIC     product_category_name_english
# MAGIC   from silver.product_category_translation
# MAGIC ),
# MAGIC item_enriched as (
# MAGIC   select
# MAGIC     i.order_id,
# MAGIC     i.product_id,
# MAGIC     i.seller_id,
# MAGIC     i.item_price,
# MAGIC     i.freight_value,
# MAGIC     i.shipping_limit_ts,
# MAGIC     o.customer_id,
# MAGIC     o.order_status,
# MAGIC     o.order_purchase_ts,
# MAGIC     o.delivered_customer_ts,
# MAGIC     r.avg_review_score,
# MAGIC     p.product_category_name,
# MAGIC     c.product_category_name_english
# MAGIC   from items i
# MAGIC   join orders o
# MAGIC     on o.order_id = i.order_id
# MAGIC   left join reviews r
# MAGIC     on r.order_id = i.order_id
# MAGIC   left join products p
# MAGIC     on p.product_id = i.product_id
# MAGIC   left join cat c
# MAGIC     on c.product_category_name = p.product_category_name
# MAGIC ),
# MAGIC customer_first_purchase as (
# MAGIC   select
# MAGIC     customer_id,
# MAGIC     min(order_purchase_ts) as first_purchase_ts
# MAGIC   from orders
# MAGIC   group by customer_id
# MAGIC ),
# MAGIC item_with_new_customer_flag as (
# MAGIC   select
# MAGIC     e.*,
# MAGIC     case
# MAGIC       when e.order_purchase_ts = f.first_purchase_ts then true
# MAGIC       else false
# MAGIC     end as is_first_purchase_for_customer
# MAGIC   from item_enriched e
# MAGIC   left join customer_first_purchase f
# MAGIC     on f.customer_id = e.customer_id
# MAGIC ),
# MAGIC order_level_item_totals as (
# MAGIC   select
# MAGIC     order_id,
# MAGIC     sum(item_price) as order_items_total
# MAGIC   from items
# MAGIC   group by order_id
# MAGIC ),
# MAGIC allocation as (
# MAGIC   select
# MAGIC     e.order_id,
# MAGIC     e.product_id,
# MAGIC     e.seller_id,
# MAGIC     e.item_price,
# MAGIC     e.freight_value,
# MAGIC     e.shipping_limit_ts,
# MAGIC     e.customer_id,
# MAGIC     e.order_status,
# MAGIC     e.order_purchase_ts,
# MAGIC     e.delivered_customer_ts,
# MAGIC     e.avg_review_score,
# MAGIC     e.product_category_name,
# MAGIC     e.product_category_name_english,
# MAGIC     e.is_first_purchase_for_customer,
# MAGIC     t.order_items_total,
# MAGIC     p.paid_value,
# MAGIC     case
# MAGIC       when t.order_items_total is null or t.order_items_total = 0 then null
# MAGIC       else e.item_price / t.order_items_total
# MAGIC     end as item_share_of_order,
# MAGIC     case
# MAGIC       when t.order_items_total is null or t.order_items_total = 0 then null
# MAGIC       else p.paid_value * (e.item_price / t.order_items_total)
# MAGIC     end as allocated_paid_value
# MAGIC   from item_with_new_customer_flag e
# MAGIC   left join order_level_item_totals t
# MAGIC     on t.order_id = e.order_id
# MAGIC   left join payments p
# MAGIC     on p.order_id = e.order_id
# MAGIC )
# MAGIC select
# MAGIC   product_id,
# MAGIC   coalesce(product_category_name_english, product_category_name) as product_category,
# MAGIC   count(*) as units_sold,
# MAGIC   count(distinct order_id) as orders_count,
# MAGIC   count(distinct seller_id) as sellers_count,
# MAGIC   sum(item_price) as gmv_item_price,
# MAGIC   sum(freight_value) as total_freight_value,
# MAGIC   avg(item_price) as avg_item_price,
# MAGIC   percentile_approx(item_price, 0.5) as median_item_price,
# MAGIC   sum(coalesce(allocated_paid_value, 0.0)) as allocated_revenue_paid_value,
# MAGIC   avg(avg_review_score) as avg_review_score,
# MAGIC   sum(case when order_status = 'delivered' then 1 else 0 end) as delivered_units,
# MAGIC   sum(case when delivered_customer_ts is not null and shipping_limit_ts is not null and delivered_customer_ts <= shipping_limit_ts then 1 else 0 end) as on_time_delivered_units,
# MAGIC   sum(case when is_first_purchase_for_customer then 1 else 0 end) as units_from_new_customers,
# MAGIC   sum(case when not is_first_purchase_for_customer then 1 else 0 end) as units_from_returning_customers,
# MAGIC   min(order_purchase_ts) as first_seen_purchase_ts,
# MAGIC   max(order_purchase_ts) as last_seen_purchase_ts
# MAGIC from allocation
# MAGIC group by
# MAGIC   product_id,
# MAGIC   coalesce(product_category_name_english, product_category_name);
# MAGIC

# COMMAND ----------

