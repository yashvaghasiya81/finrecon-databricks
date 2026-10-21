# Databricks notebook source

from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "finrecon"
SOURCE_PATH = "/Volumes/workspace/finrecon/source_files"

print("FinRecon - Bronze Ingestion")
print(f"Source path: {SOURCE_PATH}")
print(f"Target schema: {CATALOG}.{SCHEMA}")


# COMMAND ----------

SOURCE_PATH = "/Volumes/workspace/finrecon/source_files"

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


    