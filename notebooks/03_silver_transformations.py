# Databricks notebook source

from pyspark.sql import functions as F
from functools import reduce

# COMMAND ----------

# 1. CONFIGURATION

CATALOG = "workspace"
SCHEMA = "finrecon"

BRONZE_SCHEMA = f"{CATALOG}.{SCHEMA}"
SILVER_SCHEMA = f"{CATALOG}.{SCHEMA}"

print("FinRecon - Silver Layer")
print(f"Source: {BRONZE_SCHEMA}")
print(f"Target: {SILVER_SCHEMA}")

# COMMAND ----------

# 2. HELPER FUNCTIONS

def clean_text_columns(df, columns):
    """Trim whitespace and convert empty strings to null."""
    for column in columns:
        df = df.withColumn(
            column,
            F.when(
                F.trim(F.col(column)) == "",
                F.lit(None).cast("string")
            ).otherwise(F.trim(F.col(column)))
        )
    return df


def quality_summary(df, table_name):
    """Report total, valid, invalid and duplicate-key counts."""
    total = df.count()
    valid = df.filter(F.col("is_valid")).count()
    invalid = total - valid

    print(f"\n{table_name}")
    print(f"Total rows: {total}")
    print(f"Valid rows: {valid}")
    print(f"Rows requiring review: {invalid}")


def write_silver(df, table_name, key_columns):
    """
    Write a Silver Delta table.
    Duplicate business keys are flagged, not silently removed.
    """
    duplicate_keys = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .select(*key_columns)
        .withColumn("has_duplicate_key", F.lit(True))
    )

    join_condition = [
        df[column] == duplicate_keys[column]
        for column in key_columns
    ]

    df = (
        df.join(duplicate_keys, on=key_columns, how="left")
        if len(key_columns) == 1
        else df.join(duplicate_keys, on=key_columns, how="left")
    )

    df = df.withColumn(
        "has_duplicate_key",
        F.coalesce(F.col("has_duplicate_key"), F.lit(False))
    )

    target = f"{SILVER_SCHEMA}.{table_name}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )

    saved = spark.table(target)
    quality_summary(saved, target)
    print(f"Saved: {target}")

# COMMAND ----------

# 3. PAYMENT LEDGER

payments = spark.table(
    f"{BRONZE_SCHEMA}.bronze_payment_ledger"
)

payments = clean_text_columns(
    payments,
    [
        "payment_id",
        "merchant_id",
        "currency",
        "gateway",
        "payment_status",
    ],
)

payments = (
    payments
    .withColumn("currency", F.upper("currency"))
    .withColumn("gateway", F.upper("gateway"))
    .withColumn("payment_status", F.upper("payment_status"))
    .withColumn("amount", F.round("amount", 2))
    .withColumn(
        "is_valid",
        F.col("payment_id").isNotNull()
        & F.col("merchant_id").isNotNull()
        & F.col("transaction_timestamp").isNotNull()
        & F.col("amount").isNotNull()
        & (F.col("amount") >= 0)
        & F.col("currency").isNotNull()
        & F.col("gateway").isNotNull()
        & F.col("payment_status").isNotNull()
    )
)

write_silver(
    payments,
    "silver_payment_ledger",
    ["payment_id"],
)

# COMMAND ----------

# 4. GATEWAY SETTLEMENTS

settlements = spark.table(
    f"{BRONZE_SCHEMA}.bronze_gateway_settlements"
)

settlements = clean_text_columns(
    settlements,
    [
        "settlement_line_id",
        "settlement_batch_id",
        "payment_id",
        "currency",
        "settlement_status",
    ],
)

settlements = (
    settlements
    .withColumn("currency", F.upper("currency"))
    .withColumn("settlement_status", F.upper("settlement_status"))
    .withColumn("gross_amount", F.round("gross_amount", 2))
    .withColumn("fee_amount", F.round("fee_amount", 2))
    .withColumn("net_amount", F.round("net_amount", 2))
    .withColumn(
        "is_valid",
        F.col("settlement_line_id").isNotNull()
        & F.col("payment_id").isNotNull()
        & F.col("gross_amount").isNotNull()
        & (F.col("gross_amount") >= 0)
        & F.col("fee_amount").isNotNull()
        & (F.col("fee_amount") >= 0)
        & F.col("net_amount").isNotNull()
        & (F.col("net_amount") >= 0)
        & F.col("currency").isNotNull()
        & F.col("settlement_status").isNotNull()
        & (
            F.abs(
                F.col("gross_amount")
                - F.col("fee_amount")
                - F.col("net_amount")
            ) <= F.lit(0.02)
        )
    )
)

write_silver(
    settlements,
    "silver_gateway_settlements",
    ["settlement_line_id"],
)

# COMMAND ----------

# 5. BANK STATEMENTS

bank = spark.table(
    f"{BRONZE_SCHEMA}.bronze_bank_statements"
)

bank = clean_text_columns(
    bank,
    [
        "bank_transaction_id",
        "bank_reference",
        "settlement_batch_id",
        "currency",
        "bank_account_id",
    ],
)

bank = (
    bank
    .withColumn("currency", F.upper("currency"))
    .withColumn("credit_amount", F.round("credit_amount", 2))
    .withColumn("debit_amount", F.round("debit_amount", 2))
    .withColumn(
        "is_valid",
        F.col("bank_transaction_id").isNotNull()
        & F.col("transaction_timestamp").isNotNull()
        & F.col("credit_amount").isNotNull()
        & (F.col("credit_amount") >= 0)
        & F.col("debit_amount").isNotNull()
        & (F.col("debit_amount") >= 0)
        & F.col("currency").isNotNull()
        & ~(
            (F.col("credit_amount") > 0)
            & (F.col("debit_amount") > 0)
        )
    )
)

write_silver(
    bank,
    "silver_bank_statements",
    ["bank_transaction_id"],
)

# COMMAND ----------

# 6. FEE SCHEDULE

fees = spark.table(
    f"{BRONZE_SCHEMA}.bronze_fee_schedule"
)

fees = clean_text_columns(
    fees,
    ["fee_rule_id", "gateway", "currency"],
)

fees = (
    fees
    .withColumn("gateway", F.upper("gateway"))
    .withColumn("currency", F.upper("currency"))
    .withColumn("fixed_fee", F.col("fixed_fee").cast("double"))
    .withColumn(
        "is_valid",
        F.col("fee_rule_id").isNotNull()
        & F.col("gateway").isNotNull()
        & F.col("currency").isNotNull()
        & F.col("fee_percentage").isNotNull()
        & (F.col("fee_percentage") >= 0)
        & F.col("fixed_fee").isNotNull()
        & (F.col("fixed_fee") >= 0)
        & F.col("effective_from").isNotNull()
        & (
            F.col("effective_to").isNull()
            | (F.col("effective_to") >= F.col("effective_from"))
        )
    )
)

write_silver(
    fees,
    "silver_fee_schedule",
    ["fee_rule_id"],
)

# COMMAND ----------

# 7. FINAL VERIFICATION

silver_tables = [
    "silver_payment_ledger",
    "silver_gateway_settlements",
    "silver_bank_statements",
    "silver_fee_schedule",
]

print("\nFinRecon Silver Layer - Final Summary")

for table_name in silver_tables:
    full_name = f"{SILVER_SCHEMA}.{table_name}"
    df = spark.table(full_name)

    print(f"\n{full_name}")
    print(f"Rows: {df.count()}")
    print(f"Duplicate-key rows: {df.filter(F.col('has_duplicate_key')).count()}")
    print(f"Invalid rows: {df.filter(~F.col('is_valid')).count()}")

    display(df.limit(5))
