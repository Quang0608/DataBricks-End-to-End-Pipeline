# Databricks notebook source
# MAGIC %pip install databricks-labs-dqx==0.10.0
# MAGIC dbutils.library.restartPython()
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ecommerce;

# COMMAND ----------

# %sql
# CREATE TABLE IF NOT EXISTS silver.geolocation (
#     geolocation_zip_code_prefix STRING,
#     geolocation_lat DOUBLE,
#     geolocation_lng DOUBLE,
#     geolocation_city STRING,
#     geolocation_state STRING,

#     EffectiveFrom              TIMESTAMP,
#     EffectiveTo                TIMESTAMP,
#     IsActive                   BOOLEAN
# )
# USING DELTA
# TBLPROPERTIES (
#     delta.autoOptimize.optimizeWrite = true,
#     delta.autoOptimize.autoCompact = true
# );


# COMMAND ----------

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
SOURCE_TABLE = "bronze.geolocation"
TARGET_TABLE = "silver.gelocation"
CHECKPOINT_PATH = "/Volumes/ecommerce/silver/checkpoints/geolocation"
CATALOG = "ecommerce"

# COMMAND ----------

from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule
from databricks.labs.dqx import check_funcs

# --------------------
# SOURCE checks
# --------------------
source_checks = [
    DQRowRule(
        name="zip_not_null",
        column="geolocation_zip_code_prefix",
        check_func=check_funcs.is_not_null,
        criticality="error"
    ),
    DQRowRule(
        name="lat_not_null",
        column="geolocation_lat",
        check_func=check_funcs.is_not_null,
        criticality="error"
    ),
    DQRowRule(
        name="lng_not_null",
        column="geolocation_lng",
        check_func=check_funcs.is_not_null,
        criticality="error"
    )
]






# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql import DataFrame
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

def process_batch(batch_df: DataFrame, batch_id: int):

    if batch_df.isEmpty():
        return
    
    ws = WorkspaceClient()
    dq_engine = DQEngine(ws)
    validated_df = dq_engine.apply_checks(
        batch_df,
        source_checks
    )
#Lineage
    validated_df = (
        validated_df
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("processed_at", F.current_timestamp())
    )
    valid_df = validated_df.filter(F.col("_errors").isNull())
    source_invalid_df = validated_df.filter(F.col("_errors").isNotNull())

    # -----------------------------
    # Deduplicate identical rows
    # -----------------------------
    dedup_df = (
        valid_df
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy(
                    "geolocation_zip_code_prefix",
                    "geolocation_lat",
                    "geolocation_lng",
                    "geolocation_city",
                    "geolocation_state"
                )
                .orderBy("_InsertTimeStamp")
            )
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # -----------------------------
    # Basic type casting
    # -----------------------------
    clean_df = (
        dedup_df
        .select(
            "geolocation_zip_code_prefix",
            F.col("geolocation_lat").cast("double"),
            F.col("geolocation_lng").cast("double"),
            "geolocation_city",
            "geolocation_state",
            "_InsertTimeStamp",
            "batch_id",
            "processed_at"
        )
    )

    # -----------------------------
    # Write append-only
    # -----------------------------
    (
        clean_df
        .write
        .mode("append")
        .saveAsTable("ecommerce.silver.geolocation")
    )

    source_invalid_df.write.mode("append").saveAsTable(
        f"ecommerce.silver.geolocation_invalid"
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

