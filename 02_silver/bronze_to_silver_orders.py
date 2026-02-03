# Databricks notebook source
# MAGIC %pip install databricks-labs-dqx==0.10.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ecommerce

# COMMAND ----------

from pyspark.sql import functions as F, Window, DataFrame
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule

# COMMAND ----------

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
SOURCE_TABLE = "bronze.orders"
TARGET_TABLE = "silver.orders"
CHECKPOINT_PATH = "/Volumes/ecommerce/silver/checkpoints/orders"
CATALOG = "ecommerce"

# COMMAND ----------

orders_source_checks = [
    DQRowRule(
        name = "order_id_not_null",
        column= "order_id",
        check_func = check_funcs.is_not_null,
        criticality="error"
    ),
    DQRowRule(
        name = "order_status_valid",
        column = "order_status",
        check_func = check_funcs.is_not_null_and_is_in_list,
        check_func_kwargs={
            "allowed" : [
                "created", "approved", "invoiced",
                "shipped", "delivered",
                "canceled", "unavailable", "processing"
            ]
        },
        criticality="warn"
    ),
    DQRowRule(
        name = "purchase_ts_not_null",
        column = "order_purchase_timestamp",
        check_func = check_funcs.is_not_null,
        criticality = "error"
    )
    
]

target_checks = [
        DQDatasetRule(
        name = "order_id_unique",
        columns = ["order_id"],
        check_func=check_funcs.is_unique,
        criticality="error"
    ),
        DQRowRule(
        name = "carrier_after_approval",
        criticality="warn",
        check_func = check_funcs.sql_expression,
        check_func_kwargs={
            "expression": (
                "(order_delivered_carrier_date IS NULL)"
                "OR (order_approved_at IS NULL)"
                "OR (order_delivered_carrier_date >= order_approved_at)"
            ),
            "msg": "Carrier date must be after approval date"
        }
    )
]

# COMMAND ----------

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    ws = WorkspaceClient()
    dq_engine = DQEngine(ws)
    validated_df = dq_engine.apply_checks(
        batch_df,
        orders_source_checks
    )
#Lineage
    validated_df = (
        validated_df
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("processed_at", F.current_timestamp())
    )
    valid_df = validated_df.filter(F.col("_errors").isNull())
    source_invalid_df = validated_df.filter(F.col("_errors").isNotNull())

#Transform and cast
    transformed_df = (
        valid_df
        .select(
            "order_id",
            "customer_id",
            "order_status",
            F.to_timestamp("order_purchase_timestamp").alias("order_purchase_ts"),
            F.to_timestamp("order_approved_at").alias("order_approved_ts"),
            F.to_timestamp("order_delivered_carrier_date").alias("order_delivered_carrier_ts"),
            F.to_timestamp("order_delivered_customer_date").alias("order_delivered_customer_ts"),
            F.to_timestamp("order_estimated_delivery_date").alias("order_estimated_delivery_ts"),
            "_InsertTimeStamp",
            "_source_file",
            "batch_id",
            "processed_at"

        )
    )
    window_spec = (
        Window.partitionBy("order_id")
        .orderBy(F.col("_InsertTimeStamp").desc())
    )

    dedup_df = (
        transformed_df
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    fully_validated_df = dq_engine.apply_checks(
        dedup_df,
        target_checks
    )
    final_valid_df = fully_validated_df.filter(F.col("_errors").isNull())
    target_invalid_df = fully_validated_df.filter(F.col("_errors").isNotNull())

    final_valid_df.drop("_errors", "_warnings").write.mode("append").saveAsTable(TARGET_TABLE)
    
    source_invalid_df.write.mode("append").saveAsTable(
        f"{CATALOG}.{SILVER_SCHEMA}.orders_invalid_source"
    )

    target_invalid_df.write.mode("append").saveAsTable(
        f"{CATALOG}.{SILVER_SCHEMA}.orders_invalid_target"
    )

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




# COMMAND ----------

