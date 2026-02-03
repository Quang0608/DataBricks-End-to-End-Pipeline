# Databricks notebook source
# MAGIC %pip install databricks-labs-dqx==0.10.0
# MAGIC dbutils.library.restartPython()
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ecommerce;

# COMMAND ----------

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule

source_checks = [

    # ---------- Primary key ----------
    DQRowRule(
        name="product_name_not_null",
        column="product_category_name",
        check_func=check_funcs.is_not_null,
        criticality="error"
    ),

    DQRowRule(
        name="product_name_english_not_null",
        column="product_category_name_english",
        check_func=check_funcs.is_not_null,
        criticality="error"
    )

]

target_checks = [
    DQDatasetRule(
        name="category_name_unique",
        check_func=check_funcs.is_unique,
        criticality="error",
        columns=["product_category_name"]
    )
]



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ecommerce.silver.product_category_translation (
# MAGIC   product_category_name STRING NOT NULL,
# MAGIC   product_category_name_english STRING,
# MAGIC   _InsertTimeStamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import DataFrame

def merge_to_target(transformed_df: DataFrame, spark):
    if transformed_df.isEmpty():
        return

    target = DeltaTable.forName(
        spark,
        "ecommerce.silver.product_category_translation"
    )

    (
        target.alias("t")
        .merge(
            transformed_df.alias("s"),
            "t.product_category_name = s.product_category_name"
        )
        .whenMatchedUpdate(set={
            "product_category_name_english": "s.product_category_name_english",
            "_InsertTimeStamp": "s._InsertTimeStamp"
        })
        .whenNotMatchedInsert(values={
            "product_category_name": "s.product_category_name",
            "product_category_name_english": "s.product_category_name_english",
            "_InsertTimeStamp": "s._InsertTimeStamp"
        })
        .execute()
    )


# COMMAND ----------

from pyspark.sql import functions as F
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

def batch_transform(batch_df: DataFrame, batch_id: int):

    spark = batch_df.sparkSession

    ws = WorkspaceClient()
    dq_engine = DQEngine(ws)

    # ----------------------------
    # 1️⃣ SOURCE DQ
    # ----------------------------
    validated_source = dq_engine.apply_checks(batch_df, source_checks)

    clean_source = validated_source.filter(F.col("_errors").isNull())
    source_invalid = validated_source.filter(F.col("_errors").isNotNull())

    source_invalid.write.mode("append").saveAsTable(
        "ecommerce.silver.product_category_translation_invalid"
    )

    # ----------------------------
    # 2️⃣ TRANSFORM
    # ----------------------------
    transformed_df = (
        clean_source
        .select(
            "product_category_name",
            "product_category_name_english",
            F.col("_InsertTimeStamp")
        )
        .dropDuplicates(["product_category_name"])
    )

    # ----------------------------
    # 3️⃣ TARGET DQ
    # ----------------------------
    validated_target = dq_engine.apply_checks(
        transformed_df,
        target_checks
    )

    clean_target = validated_target.filter(F.col("_errors").isNull())

    # ----------------------------
    # 4️⃣ MERGE (SCD1)
    # ----------------------------
    merge_to_target(clean_target, spark)


# COMMAND ----------

checkpoint_path = "/Volumes/ecommerce/silver/checkpoints/product_category_translation"

(
    spark.readStream
    .table("bronze.product_category_translation")
    .writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", checkpoint_path)
    .foreachBatch(batch_transform)
    .start()
    .awaitTermination()
)
