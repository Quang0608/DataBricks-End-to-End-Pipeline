
# End-to-End E-Commerce Data Engineering Pipeline

An end-to-end data engineering project built on **Databricks, PySpark, Spark Structured Streaming, and Delta Lake**, using the Medallion Architecture to transform raw e-commerce data into validated, analytics-ready datasets.

The project demonstrates incremental ingestion, data quality enforcement, Slowly Changing Dimensions (SCD Type 2), dimensional data modeling, and business intelligence reporting.

## 1. Project Overview

The pipeline processes the **Brazilian E-Commerce Public Dataset by Olist**, containing information about orders, customers, sellers, products, payments, reviews, and delivery locations.

The objective is to build a structured, reliable data processing workflow that converts raw transactional data into datasets supporting business analysis.

### Key Capabilities

- Incremental CSV ingestion using Databricks Auto Loader
- Bronze, Silver, and Gold data architecture
- Spark Structured Streaming with checkpointing and micro-batch processing
- Data validation, cleansing, and invalid-record handling
- Historical dimension tracking using SCD Type 2
- Delta Lake storage and merge operations
- Star-schema fact and dimension tables
- SQL views for sales, customer, and product analytics

## 2. Technology Stack

| Component | Technology |
|-----------|------------|
| Data Platform | Databricks |
| Processing | PySpark, Spark SQL |
| Streaming | Spark Structured Streaming, Auto Loader |
| Storage | Delta Lake |
| Data Quality | Databricks Labs DQX |
| Historical Tracking | SCD Type 2, Delta MERGE |
| Data Modeling | Star Schema |
| Analytics | Databricks SQL |
| Data Source | Olist Brazilian E-Commerce Dataset |

## 3. Pipeline Architecture

```text
                 Olist CSV Datasets
                         |
                         v
                BRONZE LAYER
             Databricks Auto Loader
               Raw Delta Tables
              Ingestion Metadata
                         |
                         v
                SILVER LAYER
            Spark Structured Streaming
                foreachBatch
             Data Quality Validation
             Cleaning & Deduplication
              SCD Type 2 Processing
                         |
                         v
                 GOLD LAYER
              Star Schema Modeling
               Fact Tables
             Dimension Tables
                         |
                         v
                BUSINESS LAYER
              SQL Analytical Views
            Revenue / Product / Customer
                         |
                         v
                   DASHBOARDS
```

The pipeline uses triggered incremental processing with `availableNow=True`, rather than a permanently running continuous stream.

## 4. Medallion Architecture

### Bronze Layer — Raw Data Ingestion

The Bronze layer ingests source CSV files using Databricks Auto Loader.

Implementation:
- Read newly discovered files using the `cloudFiles` source.
- Infer or manage source schemas.
- Add ingestion timestamps and source-file metadata.
- Write raw records into Delta tables.
- Maintain checkpoints for incremental processing.

Example:

```python
(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .load(RAW_PATH)
    .writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", CHECKPOINT)
    .toTable(BRONZE_TABLE)
)
```

### Silver Layer — Data Cleaning and Validation

The Silver layer transforms raw data into standardized, validated datasets.

Main operations:
- Enforce required-field and business validation rules.
- Standardize data types and timestamps.
- Deduplicate records using business keys.
- Separate invalid records for investigation.
- Apply SCD Type 2 logic to track historical changes.
- Use streaming micro-batches and Delta Lake operations.

The implementation uses **Databricks Labs DQX** to define and apply data quality rules.

### Gold Layer — Dimensional Data Modeling

The Gold layer organizes processed data into analytical fact and dimension tables using Spark SQL and Delta Lake.

**Fact tables**
- Orders
- Order items
- Order payments
- Order reviews

**Dimension tables**
- Customers
- Products
- Sellers
- Geolocation
- Date

Fact tables capture transactional activity, while dimension tables provide descriptive attributes for analytical queries.

The Gold layer is built primarily using SQL transformations over the processed Silver datasets.

## 5. Spark Structured Streaming

Structured Streaming is used for incremental processing across the ingestion and transformation stages.

The pipeline uses:

- `readStream` to consume incoming files or Delta records.
- `writeStream` to process incremental data.
- `foreachBatch` for custom micro-batch transformations.
- Checkpointing to track streaming progress.
- `availableNow=True` to process available data and terminate.

Example:

```python
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
```

This design supports scheduled incremental processing without requiring a continuously running cluster.

## 6. Slowly Changing Dimensions (SCD Type 2)

The pipeline implements SCD Type 2 transformation logic in the Silver layer to preserve historical versions of changing dimension attributes.

Historical records use fields such as:

| Column | Description |
|--------|-------------|
| Business key | Identifies the source entity |
| EffectiveFrom | Start timestamp of the record version |
| EffectiveTo | End timestamp of the record version |
| IsActive | Indicates whether the version is current |

Delta Lake `MERGE` operations are used to expire previous versions and insert new records.

This supports analysis of historical entity attributes instead of relying only on their most recent values.

## 7. Business Analytics

The Business layer provides analytical views derived from the transformed datasets.

### Revenue Analytics

- Daily revenue trends
- Order counts
- Average order values
- Revenue performance

### Product Analytics

- Product sales performance
- Revenue by product
- Seller activity
- Product review metrics

### Customer Analytics

- Customer purchase behavior
- Customer acquisition trends
- Geographic distribution

The repository also contains SQL queries for building dashboards covering revenue trends, top products, top sellers, and new customer activity.

## 8. Repository Structure

```text
DataBricks-End-to-End-Pipeline/
|
├── 01_bronze/
│   └── raw_to_bronze_*.py
|
├── 02_silver/
│   └── bronze_to_silver_*.py
|
├── 03_gold/
│   ├── dim_*.py
│   └── silver_to_gold_*.py
|
├── Business_view/
│   ├── Daily_revenue_analytics.py
│   ├── products_performance.py
│   └── sale by customer.py
|
├── Dashboards/
│   └── SQL Queries/
|
└── README.md
```

## 9. Running the Project

### Prerequisites

- Access to a Databricks workspace
- A cluster or compatible compute environment
- Unity Catalog permissions to create or modify the required objects
- Olist CSV datasets
- Databricks Labs DQX for Silver-layer quality checks

### Execution Workflow

**Step 1 — Prepare the environment**

Configure the `ecommerce` catalog and required schemas, tables, and volumes.

Update source paths and checkpoint locations to match your workspace.

**Step 2 — Run Bronze notebooks**

Execute the notebooks in `01_bronze/` to ingest source files into Bronze Delta tables.

**Step 3 — Run Silver notebooks**

Execute the notebooks in `02_silver/` to apply data quality checks, transformations, and historical tracking.

**Step 4 — Build Gold tables**

Execute the scripts in `03_gold/`, ensuring dimension-table dependencies are available before dependent fact tables.

**Step 5 — Create business views**

Execute the notebooks in `Business_view/` to expose analytical datasets.

**Step 6 — Run dashboard queries**

Use the SQL queries in `Dashboards/SQL Queries/` to explore business performance.

## 10. Engineering Considerations

This project is an educational implementation of common lakehouse data engineering patterns.

Important considerations for production hardening include:

- Maintaining durable checkpoints between scheduled runs
- Verifying idempotent processing and replay behavior
- Validating SCD Type 2 history across multiple updates
- Handling source schema changes safely
- Testing referential integrity between fact and dimension tables
- Monitoring data freshness, completeness, and pipeline failures

## 11. Dataset

**Brazilian E-Commerce Public Dataset by Olist**

The dataset provides anonymized transactional information from a Brazilian e-commerce platform.

Source:
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## 12. Project Outcomes

This project demonstrates the design and implementation of an end-to-end Databricks data pipeline, covering raw data ingestion, transformation, validation, historical tracking, dimensional modeling, and analytics delivery.

The main learning outcomes include:

- Designing a multi-layer data processing architecture
- Implementing incremental pipelines with Structured Streaming
- Building data quality controls into ETL workflows
- Managing historical records using SCD Type 2
- Creating analytical datasets suitable for business reporting
```
