"""
================================================================================
Pragmatic Lakehouse Architecture (PLA)
PLA Open Blueprint: PLA-OBP-003-v01
Title: Classify Once. Enforce Everywhere.
File: PLA-OBP-003-v01-classify-once-enforce-everywhere.py
================================================================================
DISCLAIMER: This is illustrative sample code only. It is not production-ready
and does not represent a working model for every environment. It is provided
to give a conceptual understanding of the pattern. Copper teams may choose
different approaches depending on their platform, tooling, and organizational
structure. Better implementations may exist depending on your requirements.
================================================================================
"""

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def get_catalog_classifications(table_name):
    """
    Retrieve field classifications from the enterprise catalog.
    In practice, replace with your catalog API call (Atlan, Collibra, Alation, etc.)
    Returns: dict of {field_name: {type, classification, reference_data, allowed_values}}
    """
    return {
        "transaction_id":    {"type": "string",       "classification": "internal"},
        "business_unit":     {"type": "string",       "classification": "internal",    "master_data": "business_units"},
        "amount":            {"type": "decimal(18,2)", "classification": "confidential"},
        "currency":          {"type": "string",       "classification": "internal",    "reference_data": "iso_currencies"},
        "transaction_date":  {"type": "date",         "classification": "internal"},
        "recognition_policy":{"type": "string",       "classification": "internal",    "allowed_values": ["ASC606", "IFRS15"]},
        "customer_name":     {"type": "string",       "classification": "pii"},
    }


def get_reference_values(reference_table):
    """Retrieve valid values from the enterprise reference data store."""
    return spark.table(f"copper.reference.{reference_table}") \
               .select("code").rdd.flatMap(lambda x: x).collect()


def get_master_values(master_table):
    """Retrieve valid identifiers from the enterprise master data store."""
    return spark.table(f"copper.master.{master_table}") \
               .select("code").rdd.flatMap(lambda x: x).collect()


def apply_copper_enforcement(df, catalog):
    """
    Apply schema enforcement and classification-driven security at the Copper zone.
    In this example, both are applied in one pass — Copper teams may choose
    different approaches depending on their platform and tooling.
    """

    # ── Schema enforcement — driven by catalog type definitions ──
    df = df.withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))
    df = df.withColumn("transaction_date", F.col("transaction_date").cast("date"))

    if "currency" in catalog and catalog["currency"].get("reference_data"):
        valid_currencies = get_reference_values(catalog["currency"]["reference_data"])
        df = df.filter(F.col("currency").isin(valid_currencies))

    if "business_unit" in catalog and catalog["business_unit"].get("master_data"):
        valid_units = get_master_values(catalog["business_unit"]["master_data"])
        df = df.filter(F.col("business_unit").isin(valid_units))

    if "recognition_policy" in catalog and catalog["recognition_policy"].get("allowed_values"):
        df = df.filter(F.col("recognition_policy").isin(
            catalog["recognition_policy"]["allowed_values"]
        ))

    # ── Classification-driven security — catalog classification drives the rule ──
    for field, meta in catalog.items():
        classification = meta.get("classification")
        if classification == "pii":
            df = df.withColumn(field, F.sha2(F.col(field).cast("string"), 256))
        elif classification == "restricted":
            df = df.drop(field)

    # ── Contract enforcement — fail before bad data reaches Silver ──
    violations = df.filter(F.col("amount").isNull() | F.col("transaction_id").isNull())
    if violations.count() > 0:
        raise ValueError(
            f"Copper-Silver Data Contract violation: {violations.count()} records "
            f"failed enforcement. Pipeline halted. Review catalog classifications."
        )

    return df


# ── Main execution ────────────────────────────────────────────────
df_bronze = spark.read.format("delta") \
    .load("abfss://bronze@your-storage-account.dfs.core.windows.net/sales_transactions")

catalog = get_catalog_classifications("sales_transactions")
df_copper = apply_copper_enforcement(df_bronze, catalog)

df_copper.write.format("delta").mode("overwrite") \
    .save("abfss://copper@your-storage-account.dfs.core.windows.net/sales_transactions")

print(f"Copper enforcement complete. {df_copper.count()} records written.")
