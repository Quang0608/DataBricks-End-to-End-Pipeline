# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog ecommerce;

# COMMAND ----------

from pyspark.sql import functions as F

RAW_PATH = "/Volumes/ecommerce/bronze/raw_data/sellers/"
SCHEMA_PATH = "/Volumes/ecommerce/bronze/checkpoints/sellers_schema"
CHECKPOINT_PATH = "/Volumes/ecommerce/bronze/checkpoints/sellers"

BRONZE_TABLE = "ecommerce.bronze.sellers"

(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .option("header", "true")
    .load(RAW_PATH)
    .withColumn("_InsertTimeStamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .toTable(BRONZE_TABLE)
).awaitTermination()
