# Databricks notebook source
# MAGIC %pip install databricks-labs-dqx==0.10.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import functions as F, Window, DataFrame
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule


# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ecommerce

# COMMAND ----------

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
SOURCE_TABLE = "bronze.order_items"
TARGET_TABLE = "silver.order_items"
CHECKPOINT_PATH = "/Volumes/ecommerce/silver/checkpoints/order_items"


# COMMAND ----------

BRONZE_SOURCE_CHECKS = [
    DQRowRule(
        name="order_id_not_null",
        check_func=check_funcs.is_not_null,
        column="order_id",
        criticality="error"
    ),
    DQRowRule(
        name="order_item_id_not_null",
        check_func=check_funcs.is_not_null,
        column="order_item_id",
        criticality="error"
    ),
    DQRowRule(
        name="product_id_not_null",
        check_func=check_funcs.is_not_null,
        column="product_id",
        criticality="error"
    ),
     DQDatasetRule(
        name='CustomerKey_is_unique'
        ,check_func=check_funcs.is_unique
        ,criticality='error'
        ,columns=['order_id','order_item_id']
    ),
    DQRowRule(
        name="price_must_be_positive",
        criticality="error",
        check_func=check_funcs.sql_expression,
        check_func_kwargs={
            "expression": "price > 0",
            "msg": "price must be greater than 0"
        }
    ),

    DQRowRule(
        name="freight_value_non_negative",
        criticality="warn",
        check_func=check_funcs.sql_expression,
        check_func_kwargs={
            "expression": "freight_value >= 0",
            "msg": "freight_value must be >= 0"
        }
    ),
]

# ----------------------------
# Target DQ (Silver-level)
# ----------------------------
SILVER_TARGET_CHECKS = [
    DQRowRule(
        name="price_must_be_positive",
        check_func=check_funcs.sql_expression,
        criticality="error",
        check_func_kwargs={
            "expression": "price >= 0",
            "msg": "price must be >= 0"
        }
    ),
    DQDatasetRule(
        name="order_item_unique",
        check_func=check_funcs.is_unique,
        columns=["order_id","product_id", "order_item_id"],
        criticality="error"
    )
]

# ----------------------------
# Batch logic
# ----------------------------
def process_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return
    processed_at = F.current_timestamp()

    spark = batch_df.sparkSession
    ws = WorkspaceClient()
    dq_engine = DQEngine(ws)

    
    # ---- Source checks
    validated_source = dq_engine.apply_checks(batch_df, BRONZE_SOURCE_CHECKS)
    clean_source = validated_source.filter(F.col("_errors").isNull())
    invalid_df = validated_source.filter(F.col("_errors").isNotNull())
    # ---- Transform & cast
    transformed_df = (
        clean_source
        .withColumn("order_item_id", F.col("order_item_id").cast("int"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("freight_value", F.col("freight_value").cast("double"))
        .withColumn(
            "shipping_limit_date",
            F.to_timestamp("shipping_limit_date")
        )
        .withColumn(
            "_row_number",
            F.row_number().over(
                Window.partitionBy("order_id", "order_item_id")
                .orderBy(F.col("_ingest_time").desc())
            )
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )
    transformed_df = (
        transformed_df
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("processed_at", F.current_timestamp())
    )
    # ---- Target checks
    validated_target = dq_engine.apply_checks(
        transformed_df,
        SILVER_TARGET_CHECKS
    )

    silver_clean = validated_target.filter(F.col("_errors").isNull())

    (
        silver_clean
        .drop("_errors", "_warnings")
        .write
        .mode("append")
        .saveAsTable(TARGET_TABLE)
    )
    invalid_df.write.mode("append").saveAsTable(
        f"ecommerce.{SILVER_SCHEMA}.order_items_invalid"
    )

# ----------------------------
# Streaming job
# ----------------------------
(
    spark.readStream
    .table(SOURCE_TABLE)
    .writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .foreachBatch(process_batch)
    .start()
    .awaitTermination()
)