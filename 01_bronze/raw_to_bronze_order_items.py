# Databricks notebook source
from pyspark.sql import functions as F


# COMMAND ----------

CATALOG = "ecommerce"

BASE_VOLUME = "/Volumes/ecommerce"

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"


# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ecommerce;

# COMMAND ----------


RAW_PATH = f"{BASE_VOLUME}/bronze/raw_data/order_items/"
CHECKPOINT = "/Volumes/ecommerce/bronze/checkpoints/order_items"
SCHEMA_PATH = "/Volumes/ecommerce/bronze/checkpoints/order_items_schemas"

(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .option("header", "true")
    .option("cloudFiles.inferColumnTypes" , "true")
    .load(RAW_PATH)
    .withColumn("_ingest_time", F.current_timestamp())
    .withColumn("_source_file",F.col("_metadata.file_path"))
    .writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("append")
    .toTable("bronze.order_items")
).awaitTermination()