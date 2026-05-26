"""
================================================================================
Pragmatic Lakehouse Architecture (PLA)
PLA Open Blueprint: PLA-OBP-002-v01
Title: Governing the Zone Boundaries
File: PLA-OBP-002-v01-sales-silver-autonomous.py
================================================================================
DISCLAIMER: This is illustrative skeleton code only. It is not production-ready.
Placeholder functions marked with [IMPLEMENT] require domain-specific
implementation based on your platform and tooling.
================================================================================

Sales domain — Autonomous pipeline pattern:
  - Runs on its own schedule — no dependency on Copper DAG
  - Checks _contract_metadata for new PASSED records
    using last_processed_seq in the WHERE clause
  - Trusts Copper enforcement — no re-validation of any field
  - Applies Sales domain transformations
  - Updates its own last_processed_seq after successful run
  - Exits gracefully if no new records — retries on next schedule
  - No Airflow DAG needed — can be triggered by any scheduler
"""

from pyspark.sql import functions as F
from datetime import datetime

COPPER_PATH   = "abfss://copper@your-storage-account.dfs.core.windows.net/sales_transactions"
METADATA_PATH = "abfss://copper@your-storage-account.dfs.core.windows.net/_contract_metadata"
SALES_SILVER  = "abfss://silver@your-storage-account.dfs.core.windows.net/sales/pipeline"
SALES_META    = "abfss://silver@your-storage-account.dfs.core.windows.net/sales/_domain_metadata"
CONTRACT_NAME     = "sales_transactions_copper_silver_data_contract"
CONTRACT_VERSION  = "1.0.0"
TABLE_NAME        = "sales_transactions"


def get_last_processed_seq():
    """Get Sales domain last_processed_seq from its own metadata."""
    try:
        return spark.read.format("delta").load(SALES_META) \
            .filter(f"source_table = '{TABLE_NAME}'") \
            .agg(F.max("last_processed_seq").alias("last_seq")) \
            .collect()[0]["last_seq"] or 0
    except Exception:
        return 0  # First run


def apply_sales_transformations(df):
    """
    [IMPLEMENT] Sales domain transformations.
    Examples: territory mapping, product categorization,
    pipeline stage grouping, regional hierarchy.
    """
    raise NotImplementedError("[IMPLEMENT] apply_sales_transformations()")


def update_domain_metadata(copper_run_seq):
    """Update Sales domain last_processed_seq after successful run."""
    spark.createDataFrame([{
        "domain":             "sales",
        "source_table":       TABLE_NAME,
        "contract_name":      CONTRACT_NAME,
        "last_processed_seq": copper_run_seq,
        "processed_at":       datetime.utcnow().isoformat()
    }]).write.format("delta").mode("overwrite").save(SALES_META)


def run_sales_silver():

    # Step 1 — Get Sales last_processed_seq from its own domain metadata
    last_seq = get_last_processed_seq()

    # Step 2 — Check Copper metadata for new PASSED records
    # last_processed_seq in WHERE clause — one query does the handshake
    new_records = spark.read.format("delta").load(METADATA_PATH).filter(
        (F.col("contract_name")    == CONTRACT_NAME) &
        (F.col("contract_version") == CONTRACT_VERSION) &
        (F.col("status")           == "PASSED") &
        (F.col("run_seq")          >  last_seq)
    ).orderBy("run_seq").limit(1).collect()

    if not new_records:
        # No new Copper records — exit gracefully, retry on next schedule
        print(f"No new contract records since seq={last_seq}. "
              f"Sales Silver will retry on next scheduled run.")
        return

    copper_run_seq = new_records[0]["run_seq"]
    print(f"New contract record found. run_seq={copper_run_seq}. Proceeding.")

    # Step 3 — Read Copper output — trust the contract, no re-validation
    df_copper = spark.read.format("delta").load(COPPER_PATH)

    # Step 4 — Apply Sales domain transformations
    df_sales = apply_sales_transformations(df_copper)

    # Step 5 — Write Sales Silver output
    df_sales.write.format("delta").mode("overwrite").save(SALES_SILVER)

    # Step 6 — Update Sales last_processed_seq
    update_domain_metadata(copper_run_seq)
    print(f"Sales Silver complete. copper_run_seq={copper_run_seq}")


# No Airflow DAG — Sales runs on its own schedule
# Can be triggered by: cron, Azure Data Factory, Databricks Jobs, or any scheduler
if __name__ == "__main__":
    run_sales_silver()
