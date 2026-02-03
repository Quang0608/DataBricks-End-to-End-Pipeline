# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog ecommerce;

# COMMAND ----------

from pyspark.sql import functions as F
CATALOG = "ecommerce"
RAW_PATH = "/Volumes/ecommerce/bronze/raw_data/order_payments/"
BRONZE_TABLE = "bronze.order_payments"

CHECKPOINT = "/Volumes/ecommerce/bronze/checkpoints/order_payments"
SCHEMA_PATH = "/Volumes/ecommerce/bronze/checkpoints/order_payments_schemas"

(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .option("header", "true")
    .load(RAW_PATH)
    .withColumn("_InsertTimeStamp", F.current_timestamp())
    .withColumn("_source_file",  F.col("_metadata.file_path"))
    .writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("append")
    .toTable(BRONZE_TABLE)
).awaitTermination()


# COMMAND ----------

