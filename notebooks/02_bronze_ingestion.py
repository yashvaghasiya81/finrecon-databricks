# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "finrecon"
SOURCE_PATH = "/Volumes/workspace/finrecon/source_files"

print("FinRecon - Bronze Ingestion")
print(f"Source path: {SOURCE_PATH}")
print(f"Target schema: {CATALOG}.{SCHEMA}")

# COMMAND ----------

source_path = "/Volumes/workspace/finrecon/source_files"

display(dbutils.fs.ls(source_path))

# COMMAND ----------

csv_files = [
    "payment_ledger.csv",
    "gateway_settlements.csv",
    "bank_statements.csv",
    "fee_schedule.csv",
]

for file_name in csv_files:
    print(f"\n{'=' * 60}")
    print(f"File: {file_name}")
    print("=" * 60)

    file_path = f"{SOURCE_PATH}/{file_name}"

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )

    print("Schema:")
    df.printSchema()

    print("Sample records:")
    display(df.limit(5))


# COMMAND ----------

from pyspark.sql import functions as F

datasets = {
    "payment_ledger.csv": "bronze_payment_ledger",
    "gateway_settlements.csv": "bronze_gateway_settlements",
    "bank_statements.csv": "bronze_bank_statements",
    "fee_schedule.csv": "bronze_fee_schedule",
}

for file_name, table_name in datasets.items():
    file_path = f"{SOURCE_PATH}/{file_name}"
    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"

    print(f"Processing: {file_name}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )

    row_count = spark.table(full_table_name).count()

    print(f"Created table: {full_table_name}")
    print(f"Rows loaded: {row_count}")

    display(spark.table(full_table_name).limit(5))


    