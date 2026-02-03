# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog ecommerce;

# COMMAND ----------

from pyspark.sql import functions as F

RAW_PATH = "/Volumes/ecommerce/bronze/raw_data/orders/"
BRONZE_TABLE = "bronze.orders"

CHECKPOINT = "/Volumes/ecommerce/bronze/checkpoints/orders"
SCHEMA_PATH = "/Volumes/ecommerce/bronze/checkpoints/orders_schemas"
(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(RAW_PATH)
    .withColumn("_InsertTimeStamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("append")
    .toTable(BRONZE_TABLE)      

).awaitTermination()


# COMMAND ----------

