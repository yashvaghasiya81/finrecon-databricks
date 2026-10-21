# Databricks notebook source

from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "finrecon"
SOURCE_PATH = "/Volumes/workspace/finrecon/source_files"

print("FinRecon - Bronze Ingestion")
print(f"Source path: {SOURCE_PATH}")
print(f"Target schema: {CATALOG}.{SCHEMA}")