# Databricks notebook source
# MAGIC %pip install databricks-labs-dqx==0.10.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ecommerce;

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
TABLE = "order_reviews"

SOURCE_TABLE = f"{BRONZE_SCHEMA}.{TABLE}"
TARGET_TABLE = f"{SILVER_SCHEMA}.{TABLE}"
INVALID_TABLE = f"{SILVER_SCHEMA}.{TABLE}_invalid"
CHECKPOINT_PATH ="/Volumes/ecommerce/silver/checkpoints/order_reviews"

def transform_batch(batch_df: DataFrame, batch_id:int):
    if not batch_df.take(1):
        return
    
    null_violation_df = batch_df.filter(
        F.col("order_id").isNull() | 
        F.col("review_id").isNull()
    )

    not_null_df = batch_df.filter(
        F.col("order_id").isNotNull() &
        F.col("review_id").isNotNull()
    )

    dup_keys_df = (
        not_null_df.groupBy("order_id", "review_id")
        .count()
        .filter(F.col("count") > 1)
        .select("order_id", "review_id")
    )

    duplicate_violation_df = (
        not_null_df
        .join(dup_keys_df, ["order_id", "review_id"], "inner")
    )

    valid_df = (
        not_null_df
        .join(dup_keys_df, ["order_id", "review_id"], "left_anti")
    )

    parsed_df = (
        valid_df
        .withColumn(
            "review_score",
            F.when(
                F.col("review_score").rlike("^[1-5]$"),
                F.col("review_score").cast("int")
            )
        )
        .withColumn(
            "review_creation_ts",
            F.expr("try_to_timestamp(review_creation_date, 'yyyy-MM-dd HH:mm:ss')")
        )
        .withColumn(
            "review_answer_ts",
            F.expr("try_to_timestamp(review_answer_timestamp, 'yyyy-MM-dd HH:mm:ss')")
        )
    )

    format_violation_df = parsed_df.filter(
        F.col("review_score").isNull() |
        F.col("review_creation_ts").isNull()
    )

    format_violation_df = format_violation_df.withColumn("review_score", F.col("review_score").cast("string"))

    final_valid_df = parsed_df.filter(
        F.col("review_score").isNotNull() &
        F.col("review_creation_ts").isNotNull()
    )

    INVALID_COLS = [
    "order_id",
    "review_id",
    "review_score",
    "review_creation_date",
    "review_answer_timestamp",
    "_source_file",
    "_InsertTimeStamp",
    "dq_error_reason"
]

    null_violation_df = (
    null_violation_df
    .withColumn("dq_error_reason", F.lit("NULL_KEY"))
    .select(*INVALID_COLS)
)

    duplicate_violation_df = (
        duplicate_violation_df
        .withColumn("dq_error_reason", F.lit("DUPLICATE_KEY"))
        .select(*INVALID_COLS)
    )

    format_violation_df = (
        format_violation_df
        .withColumn("dq_error_reason", F.lit("INVALID_FORMAT"))
        .select(*INVALID_COLS)
    )


    invalid_df = (
        null_violation_df
        .unionByName(duplicate_violation_df, allowMissingColumns = True)
        .unionByName(format_violation_df, allowMissingColumns = True)
    )

    final_valid_df = (
        final_valid_df
        .select(
            "order_id",
            "review_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_ts",
            "review_answer_ts",
            "_source_file",
            "_InsertTimeStamp"
        )
        .withColumn("dq_batch_id", F.lit(batch_id))
        .withColumn("dq_processed_at", F.current_timestamp())
    )

    
    final_valid_df.write.mode("append").saveAsTable(TARGET_TABLE)

    
    invalid_df.withColumn("dq_batch_id", F.lit(batch_id)) \
        .withColumn("dq_processed_at", F.current_timestamp())\
        .write\
        .mode("append")\
        .saveAsTable(INVALID_TABLE)
(
    spark.readStream
    .table(SOURCE_TABLE)
    .writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .foreachBatch(transform_batch)
    .start()
    .awaitTermination()
)