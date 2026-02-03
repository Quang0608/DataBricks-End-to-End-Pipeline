# Databricks notebook source
# MAGIC %pip install databricks-labs-dqx==0.10.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ecommerce

# COMMAND ----------

from pyspark.sql import functions as F, Window
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule

CATALOG = "ecommerce"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
TABLE = "order_payments"

SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.{TABLE}"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.{TABLE}"
INVALID_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.{TABLE}_invalid"
CHECKPOINT_PATH ="/Volumes/ecommerce/silver/checkpoints/order_payments"
# -----------------------
# Source checks (raw correctness)
# -----------------------
source_checks = [
    DQRowRule(
        name="order_id_not_null",
        column="order_id",
        check_func=check_funcs.is_not_null,
        criticality="error"
    ),
    DQRowRule(
        name="payment_value_positive",
        column="payment_value",
        check_func=check_funcs.sql_expression,
        criticality="error",
        check_func_kwargs={
            "expression": "payment_value >= 0",
            "msg": "payment_value must be >= 0"
        }
    )
]

# -----------------------
# Target checks (business correctness)
# -----------------------
target_checks = [
    DQRowRule(
        name="payment_type_valid",
        column="payment_type",
        check_func=check_funcs.is_not_null_and_is_in_list,
        criticality="error",
        check_func_kwargs={
            "allowed": ["credit_card", "boleto", "voucher", "debit_card", "not_defined"]
        }
    ),
    DQDatasetRule(
        name="unique_payment",
        check_func=check_funcs.is_unique,
        criticality="error",
        columns=["order_id", "payment_sequential"]
    )
]

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    ws = WorkspaceClient()
    dq_engine = DQEngine(ws)

    # -----------------------
    # Source DQ
    # -----------------------
    validated_src = dq_engine.apply_checks(batch_df, source_checks)

    validated_src = (
        validated_src
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("processed_at", F.current_timestamp())
    )

    valid_src = validated_src.filter(F.col("_errors").isNull())
    invalid_src = validated_src.filter(F.col("_errors").isNotNull())

    # -----------------------
    # Transform + cast
    # -----------------------
    transformed = (
        valid_src
        .select(
            "order_id",
            F.col("payment_sequential").cast("int"),
            "payment_type",
            F.col("payment_installments").cast("int"),
            F.col("payment_value").cast("double"),
            "_InsertTimeStamp",
            "_source_file",
            "batch_id",
            "processed_at"
        )
    )

    # -----------------------
    # Dedup (latest wins)
    # -----------------------
    window_spec = (
        Window.partitionBy("order_id", "payment_sequential")
        .orderBy(F.col("_InsertTimeStamp").desc())
    )

    dedup_df = (
        transformed
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # -----------------------
    # Target DQ
    # -----------------------
    validated_tgt = dq_engine.apply_checks(dedup_df, target_checks)

    valid_tgt = validated_tgt.filter(F.col("_errors").isNull())
    invalid_tgt = validated_tgt.filter(F.col("_errors").isNotNull())

    # -----------------------
    # Write
    # -----------------------
    valid_tgt.drop("_errors", "_warnings") \
        .write.mode("append").saveAsTable(TARGET_TABLE)

    invalid_src.unionByName(invalid_tgt, allowMissingColumns=True) \
        .write.mode("append").saveAsTable(INVALID_TABLE)

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

