# Databricks notebook source
# MAGIC %pip install databricks-labs-dqx==0.10.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ecommerce

# COMMAND ----------

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import DQRowRule

source_checks = [

    DQRowRule(
        name="product_id_not_null",
        column="product_id",
        check_func=check_funcs.is_not_null,
        criticality="error"
    ),

    DQRowRule(
        name="category_not_null",
        column="product_category_name",
        check_func=check_funcs.is_not_null,
        criticality="warn"
    ),

    DQRowRule(
        name="insert_ts_not_null",
        column="_InsertTimeStamp",
        check_func=check_funcs.is_not_null,
        criticality="error"
    )
]


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ecommerce.silver.products (
# MAGIC     product_id STRING,
# MAGIC     product_category_name STRING,
# MAGIC     product_name_length INT,
# MAGIC     product_description_length INT,
# MAGIC     product_photos_qty INT,
# MAGIC     product_weight_g INT,
# MAGIC     product_length_cm INT,
# MAGIC     product_height_cm INT,
# MAGIC     product_width_cm INT,
# MAGIC
# MAGIC     EffectiveFrom TIMESTAMP,
# MAGIC     EffectiveTo TIMESTAMP,
# MAGIC     IsActive BOOLEAN,
# MAGIC
# MAGIC     _InsertTimeStamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F, Window

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
            Window.partitionBy("product_id")
            .orderBy("EffectiveFrom")
        )
    )

    
    transformed_df__for_update = transformed_df.filter(
        F.col("_incoming_order") == 1
    ).withColumn("_merge_key", F.col("product_id"))

    transformed_df__for_insert = transformed_df.withColumn(
        "_merge_key", F.lit(None)
    )

    final_df = transformed_df__for_update.unionByName(
        transformed_df__for_insert
    )

    (
        DeltaTable.forName(spark_session, "silver.products").alias("target")
        .merge(
            source = final_df.alias("source"),
            condition = """
                source._merge_key = target.product_id
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
                "product_category_name",
                "product_name_lenght",
                "product_description_lenght",
                "product_photos_qty",
                "product_weight_g",
                "product_length_cm",
                "product_height_cm",
                "product_width_cm"
            ),
            256
        )
    )
    .withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy(
                "product_id",
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
            "product_id",
            "product_category_name",

            F.col("product_name_lenght").cast("int").alias("product_name_length"),
            F.col("product_description_lenght").cast("int").alias("product_description_length"),
            F.col("product_photos_qty").cast("int"),
            F.col("product_weight_g").cast("int"),
            F.col("product_length_cm").cast("int"),
            F.col("product_height_cm").cast("int"),
            F.col("product_width_cm").cast("int"),

            F.col("_InsertTimeStamp").alias("EffectiveFrom"),
            F.col("_InsertTimeStamp")
        )
        .withColumn("EffectiveTo", F.lit(None).cast("timestamp"))
        .withColumn("IsActive", F.lit(True))
    )

    
    source_invalid_df.write.mode("append").saveAsTable(
        f"ecommerce.silver.products_source_invalid"
    )

    # ------------------------------------
    # 5️⃣ Merge to Silver (SCD2)
    # ------------------------------------
    merge_to_target(
        transformed_df, spark_session
    )


# COMMAND ----------

checkpoint_path = "/Volumes/ecommerce/silver/checkpoints/products"

(
    spark.readStream
    .table("ecommerce.bronze.products")
    .writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", checkpoint_path)
    .foreachBatch(batch_transform)
    .start()
    .awaitTermination()
)
