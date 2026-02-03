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
        name="customer_id_not_null",
        column="customer_id",
        check_func=check_funcs.is_not_null,
        criticality="error"
    ),

    DQDatasetRule(
        name="customer_id_unique",
        columns=["customer_id"],
        check_func=check_funcs.is_unique,
        criticality="warn"
    ),

    # ---------- customer_unique_id ----------
    DQRowRule(
        name="customer_unique_id_not_null",
        column="customer_unique_id",
        check_func=check_funcs.is_not_null,
        criticality="error"
    ),

]


# COMMAND ----------

target_checks = [

    # ---------- Business key ----------
    DQRowRule(
        name="customer_id_not_null",
        column="customer_id",
        check_func=check_funcs.is_not_null,
        criticality="error"
    ),

    # ---------- SCD2 correctness ----------
    DQRowRule(
        name="effective_from_not_null",
        column="EffectiveFrom",
        check_func=check_funcs.is_not_null,
        criticality="error"
    ),

    DQRowRule(
    name="active_flag_effectivet_to_consistency",
    check_func=check_funcs.sql_expression,
    criticality="error",
    check_func_kwargs={
        "expression": "(IsActive = true AND EffectiveTo IS NULL) OR (IsActive = false AND EffectiveTo IS NOT NULL)",
        "msg": "Each row must have IsActive and EffectiveTo consistent"
    }
)
,

    # ---------- City / State ----------
    DQRowRule(
        name="customer_city_not_null",
        column="customer_city",
        check_func=check_funcs.is_not_null,
        criticality="warn"
    ),

    DQRowRule(
        name="customer_state_valid",
        column="customer_state",
        check_func=check_funcs.is_not_null,
        criticality="warn"
    )
]


# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

def merge_to_target(transformed_df, spark_session):
    if transformed_df.isEmpty():
        return
    insert_columns = [
        c for c in transformed_df.columns
        if c not in ["_incoming_order", "_merge_key"]
    ]

    insert_mapping = {
        f"target.{col}": f"source.{col}" for col in insert_columns
    }

    transformed_df = transformed_df.withColumn(
        "_incoming_order",
        F.row_number().over(
            Window.partitionBy("customer_id")
            .orderBy("EffectiveFrom")
        )
    )

    
    transformed_df__for_update = transformed_df.filter(
        F.col("_incoming_order") == 1
    ).withColumn("_merge_key", F.col("customer_id"))

    transformed_df__for_insert = transformed_df.withColumn(
        "_merge_key", F.lit(None)
    )

    final_df = transformed_df__for_update.unionByName(
        transformed_df__for_insert
    )



    (
        DeltaTable.forName(spark_session, "silver.customers").alias("target")
        .merge(
            source = final_df.alias("source"),
            condition = """
                source._merge_key = target.customer_id
                and target.IsActive = true
            """
        )
        .whenMatchedUpdate(
            set = {
                "EffectiveTo": "source.EffectiveFrom",
                "IsActive":"false"
            }
        )
        .whenNotMatchedInsert(
            condition = "source._merge_key IS NULL",
            values = insert_mapping
        )
        .execute()
    )

# COMMAND ----------

from pyspark.sql import functions as F, Window
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

def batch_transform(batch_df: DataFrame, batch_id: int):

    spark_session = batch_df.sparkSession

    # ------------------------------------
    # 1️⃣ Apply SOURCE DQ checks
    # ------------------------------------
    ws = WorkspaceClient()
    dq_engine = DQEngine(ws)

    validated_source_df = dq_engine.apply_checks(batch_df, source_checks)
    clean_source_df = validated_source_df.filter(F.col("_errors").isNull())
    source_invalid_df = validated_source_df.filter(F.col("_errors").isNotNull())


    dedup_src = (
    clean_source_df
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                "customer_unique_id",
                "customer_zip_code_prefix",
                "customer_city",
                "customer_state"
            ),
            256
        )
    )
    .withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy(
                "customer_id",
                "_InsertTimeStamp",
                "record_hash"
            ).orderBy("_InsertTimeStamp")
        )
    )
    .filter(F.col("_rn") == 1)
    .drop("_rn", "record_hash")
)

    # ------------------------------------
    # 2️⃣ Transform to Silver schema
    # ------------------------------------
    transformed_df = (
        dedup_src
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            F.col("_InsertTimeStamp").alias("EffectiveFrom")
        )
        .withColumn(
            "EffectiveTo", F.lead("EffectiveFrom").over(Window.partitionBy("customer_id").orderBy("EffectiveFrom"))
        )
        .withColumn(
            "IsActive", F.when(F.col("EffectiveTo").isNull(), True).otherwise(False)
        )
    )

    # ------------------------------------
    # 4️⃣ Apply TARGET DQ checks
    # ------------------------------------
    validated_target_df = dq_engine.apply_checks(
        transformed_df,
        target_checks
    )

    clean_target_df = validated_target_df.filter(
        F.col("_errors").isNull()
    )

    target_invalid_df = validated_target_df.filter(F.col("_errors").isNotNull())

    
    source_invalid_df.write.mode("append").saveAsTable(
        f"ecommerce.silver.customers_source_invalid"
    )

    target_invalid_df.write.mode("append").saveAsTable(
        f"ecommerce.silver.customers_target_invalid"
    )


    # ------------------------------------
    # 5️⃣ Merge to Silver (SCD2)
    # ------------------------------------
    merge_to_target(
        clean_target_df.drop("_errors", "_warnings"),
        spark_session
    )


# COMMAND ----------

checkpoint_path = "/Volumes/ecommerce/silver/checkpoints/customers"

dbutils.fs.rm(checkpoint_path, recurse=True)

(
    spark.readStream
    .table("bronze.customers")
    .writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", checkpoint_path)
    .foreachBatch(batch_transform)
    .start()
    .awaitTermination()
)


# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table silver.customers