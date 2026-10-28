# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# ============================================================
# 1. CONFIGURATION
# ============================================================

spark = SparkSession.builder.getOrCreate()

SCHEMA = "workspace.finrecon"

LEDGER_TABLE = f"{SCHEMA}.silver_payment_ledger"
GATEWAY_TABLE = f"{SCHEMA}.silver_gateway_settlements"
BANK_TABLE = f"{SCHEMA}.silver_bank_statements"

PAYMENT_OUTPUT = f"{SCHEMA}.reconciliation_results"
BATCH_OUTPUT = f"{SCHEMA}.reconciliation_batch_results"

AMOUNT_TOLERANCE = 0.01

# ============================================================
# 2. READ SILVER TABLES
# ============================================================

ledger_df = spark.table(LEDGER_TABLE)
gateway_df = spark.table(GATEWAY_TABLE)
bank_df = spark.table(BANK_TABLE)

print(f"Ledger records: {ledger_df.count()}")
print(f"Gateway records: {gateway_df.count()}")
print(f"Bank statement records: {bank_df.count()}")

# ============================================================
# 3. PAYMENT-LEVEL AGGREGATION
# ============================================================

# Aggregate payment ledger records.
ledger_payment = (
    ledger_df
    .groupBy("payment_id")
    .agg(
        F.count("*").alias("ledger_record_count"),
        F.sum("amount").alias("ledger_amount"),
        F.first("currency", ignorenulls=True).alias("ledger_currency"),
        F.first("payment_status", ignorenulls=True).alias(
            "ledger_payment_status"
        ),
        F.max(F.col("is_valid").cast("int")).alias(
            "ledger_has_valid_row"
        ),
        F.max(F.col("has_duplicate_key").cast("int")).alias(
            "ledger_has_duplicate_key"
        ),
        F.sum(
            F.when(F.col("is_valid") == False, 1).otherwise(0)
        ).alias("ledger_invalid_count")
    )
)

# Keep every gateway record for auditing.
# Only SETTLED records contribute to successful settlement amounts.
gateway_payment = (
    gateway_df
    .groupBy("payment_id")
    .agg(
        F.count("*").alias("gateway_record_count"),

        F.sum(
            F.when(
                F.col("settlement_status") == "SETTLED",
                F.col("gross_amount")
            )
        ).alias("gateway_gross_amount"),

        F.sum(
            F.when(
                F.col("settlement_status") == "SETTLED",
                F.col("fee_amount")
            )
        ).alias("gateway_fee_amount"),

        F.sum(
            F.when(
                F.col("settlement_status") == "SETTLED",
                F.col("net_amount")
            )
        ).alias("gateway_net_amount"),

        F.sum(
            F.when(
                F.col("settlement_status") == "SETTLED", 1
            ).otherwise(0)
        ).alias("settled_record_count"),

        F.sum(
            F.when(
                F.col("settlement_status") == "PENDING", 1
            ).otherwise(0)
        ).alias("pending_record_count"),

        F.sum(
            F.when(
                F.col("settlement_status") == "FAILED", 1
            ).otherwise(0)
        ).alias("failed_record_count"),

        F.first("currency", ignorenulls=True).alias(
            "gateway_currency"
        ),

        F.collect_set("settlement_batch_id").alias(
            "settlement_batch_ids"
        ),

        F.max(F.col("is_valid").cast("int")).alias(
            "gateway_has_valid_row"
        ),

        F.max(F.col("has_duplicate_key").cast("int")).alias(
            "gateway_has_duplicate_key"
        ),

        F.sum(
            F.when(F.col("is_valid") == False, 1).otherwise(0)
        ).alias("gateway_invalid_count")
    )
)

# ============================================================
# 4. PAYMENT-LEVEL RECONCILIATION
# ============================================================

payment_recon = (
    ledger_payment
    .join(gateway_payment, on="payment_id", how="full_outer")
    .withColumn(
        "amount_difference",
        F.when(
            F.col("ledger_amount").isNotNull()
            & F.col("gateway_gross_amount").isNotNull(),
            F.col("ledger_amount") - F.col("gateway_gross_amount")
        )
    )
    .withColumn(
        "currency_mismatch",
        F.col("ledger_currency").isNotNull()
        & F.col("gateway_currency").isNotNull()
        & (
            F.col("ledger_currency")
            != F.col("gateway_currency")
        )
    )
)

payment_recon = payment_recon.withColumn(
    "reconciliation_status",
    F.when(
        F.col("ledger_record_count").isNull(),
        F.lit("MISSING_LEDGER")
    )
    .when(
        F.col("gateway_record_count").isNull(),
        F.lit("MISSING_GATEWAY")
    )
    .when(
        (F.coalesce(F.col("ledger_invalid_count"), F.lit(0)) > 0)
        | (F.coalesce(F.col("gateway_invalid_count"), F.lit(0)) > 0),
        F.lit("INVALID_RECORD")
    )
    .when(
        (F.coalesce(F.col("ledger_has_duplicate_key"), F.lit(0)) == 1)
        | (F.coalesce(F.col("gateway_has_duplicate_key"), F.lit(0)) == 1),
        F.lit("DUPLICATE_KEY")
    )
    .when(
        F.col("currency_mismatch"),
        F.lit("CURRENCY_MISMATCH")
    )
    .when(
        F.coalesce(F.col("settled_record_count"), F.lit(0)) == 0,
        F.when(
            F.coalesce(F.col("pending_record_count"), F.lit(0)) > 0,
            F.lit("PENDING_SETTLEMENT")
        ).otherwise(F.lit("NO_SUCCESSFUL_SETTLEMENT"))
    )
    .when(
        F.col("ledger_amount").isNull()
        | F.col("gateway_gross_amount").isNull(),
        F.lit("MISSING_AMOUNT")
    )
    .when(
        F.abs(F.col("amount_difference")) > AMOUNT_TOLERANCE,
        F.lit("AMOUNT_MISMATCH")
    )
    .otherwise(F.lit("MATCHED"))
).withColumn(
    "reconciliation_timestamp",
    F.current_timestamp()
)

# ============================================================
# 5. GATEWAY BATCH AGGREGATION
# ============================================================

# Batch net totals include only successful SETTLED records.
gateway_batch = (
    gateway_df
    .groupBy("settlement_batch_id")
    .agg(
        F.count("*").alias("gateway_batch_record_count"),

        F.sum(
            F.when(
                F.col("settlement_status") == "SETTLED",
                F.col("net_amount")
            )
        ).alias("gateway_batch_net_amount"),

        F.sum(
            F.when(
                F.col("settlement_status") == "SETTLED", 1
            ).otherwise(0)
        ).alias("batch_settled_record_count"),

        F.sum(
            F.when(
                F.col("settlement_status") == "PENDING", 1
            ).otherwise(0)
        ).alias("batch_pending_record_count"),

        F.sum(
            F.when(
                F.col("settlement_status") == "FAILED", 1
            ).otherwise(0)
        ).alias("batch_failed_record_count"),

        F.first("currency", ignorenulls=True).alias(
            "gateway_batch_currency"
        ),

        F.max(F.col("is_valid").cast("int")).alias(
            "gateway_batch_has_valid_row"
        ),

        F.max(F.col("has_duplicate_key").cast("int")).alias(
            "gateway_batch_has_duplicate_key"
        ),

        F.sum(
            F.when(F.col("is_valid") == False, 1).otherwise(0)
        ).alias("gateway_batch_invalid_count")
    )
)

# ============================================================
# 6. BANK BATCH AGGREGATION
# ============================================================

bank_batch = (
    bank_df
    .groupBy("settlement_batch_id")
    .agg(
        F.count("*").alias("bank_record_count"),
        F.sum("credit_amount").alias("bank_credit_amount"),
        F.sum("debit_amount").alias("bank_debit_amount"),

        F.first("currency", ignorenulls=True).alias(
            "bank_currency"
        ),

        F.max(F.col("is_valid").cast("int")).alias(
            "bank_has_valid_row"
        ),

        F.max(F.col("has_duplicate_key").cast("int")).alias(
            "bank_has_duplicate_key"
        ),

        F.sum(
            F.when(F.col("is_valid") == False, 1).otherwise(0)
        ).alias("bank_invalid_count")
    )
)

# ============================================================
# 7. BATCH-LEVEL RECONCILIATION
# ============================================================

batch_recon = (
    gateway_batch
    .join(
        bank_batch,
        on="settlement_batch_id",
        how="full_outer"
    )
    .withColumn(
        "batch_amount_difference",
        F.when(
            F.col("gateway_batch_net_amount").isNotNull()
            & F.col("bank_credit_amount").isNotNull(),
            F.col("gateway_batch_net_amount")
            - F.col("bank_credit_amount")
        )
    )
    .withColumn(
        "batch_currency_mismatch",
        F.col("gateway_batch_currency").isNotNull()
        & F.col("bank_currency").isNotNull()
        & (
            F.col("gateway_batch_currency")
            != F.col("bank_currency")
        )
    )
)

batch_recon = batch_recon.withColumn(
    "reconciliation_status",
    F.when(
        F.col("gateway_batch_record_count").isNull(),
        F.lit("MISSING_GATEWAY_BATCH")
    )
    .when(
        F.col("bank_record_count").isNull(),
        F.lit("MISSING_BANK_TRANSACTION")
    )
    .when(
        (F.coalesce(F.col("gateway_batch_invalid_count"), F.lit(0)) > 0)
        | (F.coalesce(F.col("bank_invalid_count"), F.lit(0)) > 0),
        F.lit("INVALID_RECORD")
    )
    .when(
        (
            F.coalesce(
                F.col("gateway_batch_has_duplicate_key"), F.lit(0)
            ) == 1
        )
        | (
            F.coalesce(F.col("bank_has_duplicate_key"), F.lit(0)) == 1
        ),
        F.lit("DUPLICATE_KEY")
    )
    .when(
        F.col("batch_currency_mismatch"),
        F.lit("CURRENCY_MISMATCH")
    )
    .when(
        F.coalesce(F.col("batch_settled_record_count"), F.lit(0)) == 0,
        F.when(
            F.coalesce(F.col("batch_pending_record_count"), F.lit(0)) > 0,
            F.lit("PENDING_SETTLEMENT")
        ).otherwise(F.lit("NO_SUCCESSFUL_SETTLEMENT"))
    )
    .when(
        F.col("gateway_batch_net_amount").isNull()
        | F.col("bank_credit_amount").isNull(),
        F.lit("MISSING_AMOUNT")
    )
    .when(
        F.abs(F.col("batch_amount_difference")) > AMOUNT_TOLERANCE,
        F.lit("AMOUNT_MISMATCH")
    )
    .otherwise(F.lit("MATCHED"))
).withColumn(
    "reconciliation_timestamp",
    F.current_timestamp()
)

# ============================================================
# 8. WRITE DELTA OUTPUT TABLES
# ============================================================

(
    payment_recon.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(PAYMENT_OUTPUT)
)

(
    batch_recon.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BATCH_OUTPUT)
)

# ============================================================
# 9. DISPLAY RECONCILIATION SUMMARIES
# ============================================================

print("Payment-level reconciliation summary")

display(
    spark.table(PAYMENT_OUTPUT)
    .groupBy("reconciliation_status")
    .count()
    .orderBy(F.desc("count"))
)

print("Batch-level reconciliation summary")

display(
    spark.table(BATCH_OUTPUT)
    .groupBy("reconciliation_status")
    .count()
    .orderBy(F.desc("count"))
)

print("Reconciliation engine completed successfully.")
