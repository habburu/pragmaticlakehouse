"""
================================================================================
Pragmatic Lakehouse Architecture (PLA)
PLA Open Blueprint: PLA-OBP-002-v01
Title: Governing the Zone Boundaries
File: PLA-OBP-002-v01-governing-zone-boundaries.yaml (companion code)
================================================================================
DISCLAIMER: This is illustrative sample code only. It is not production-ready
and does not represent a working model for every environment. It is provided
to give a conceptual understanding of the Orchestrated and Autonomous pipeline
patterns for the Copper-Silver Data Contract handshake.
Better implementations may exist depending on your platform, tooling,
and organizational requirements.
================================================================================

TWO PATTERNS FOR COPPER-SILVER CONTRACT HANDSHAKE:

  Pattern 1 — Orchestrated (Finance domain)
  Copper triggers Finance Silver via Airflow on successful contract validation.
  Finance Silver trusts Copper's enforcement and proceeds immediately.

  Pattern 2 — Autonomous (Sales domain)
  Sales Silver runs on its own schedule, checks the contract metadata table
  for new PASSED records using its own last_processed_seq.
  Completely independent of Copper's pipeline schedule.

Both patterns:
  - Reference sales_transactions_copper_silver_data_contract v1.0.0
  - Trust Copper's enforcement — no field re-validation in Silver
  - Maintain their own last_processed_seq independently
  - Read from copper._contract_metadata

CONTRACT METADATA TABLE SCHEMA (created and owned by Copper team):
  copper._contract_metadata
  ┌─────────────┬─────────────────────────────────────────────┬──────────────────┬────────────┬────────┬─────────────────────┬───────────┬─────────────────┐
  │ run_seq     │ contract_name                               │ contract_version │ table_name │ status │ enforced_at         │ row_count │ violation_count │
  ├─────────────┼─────────────────────────────────────────────┼──────────────────┼────────────┼────────┼─────────────────────┼───────────┼─────────────────┤
  │ 1042        │ sales_transactions_copper_silver_data_...   │ 1.0.0            │ sales_...  │ PASSED │ 2026-01-15 06:00:00 │ 48291     │ 0               │
  └─────────────┴─────────────────────────────────────────────┴──────────────────┴────────────┴────────┴─────────────────────┴───────────┴─────────────────┘
"""

# ═══════════════════════════════════════════════════════════════════
# COPPER PIPELINE — shared by both patterns
# Validates data per contract YAML, writes metadata record on completion
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

CONTRACT_NAME    = "sales_transactions_copper_silver_data_contract"
CONTRACT_VERSION = "1.0.0"
TABLE_NAME       = "sales_transactions"
METADATA_PATH    = "abfss://copper@your-storage-account.dfs.core.windows.net/_contract_metadata"
COPPER_PATH      = "abfss://copper@your-storage-account.dfs.core.windows.net/sales_transactions"
BRONZE_PATH      = "abfss://bronze@your-storage-account.dfs.core.windows.net/sales_transactions"


def get_next_run_seq():
    """Get next sequence number from the metadata table."""
    try:
        df = spark.read.format("delta").load(METADATA_PATH)
        return df.agg(F.max("run_seq")).collect()[0][0] + 1
    except Exception:
        return 1  # First run


def write_contract_metadata(run_seq, status, row_count, violation_count):
    """Write contract execution record to metadata table."""
    record = [{
        "run_seq":           run_seq,
        "contract_name":     CONTRACT_NAME,
        "contract_version":  CONTRACT_VERSION,
        "table_name":        TABLE_NAME,
        "status":            status,
        "enforced_at":       datetime.utcnow().isoformat(),
        "row_count":         row_count,
        "violation_count":   violation_count
    }]
    schema = StructType([
        StructField("run_seq",           LongType(),   False),
        StructField("contract_name",     StringType(), False),
        StructField("contract_version",  StringType(), False),
        StructField("table_name",        StringType(), False),
        StructField("status",            StringType(), False),
        StructField("enforced_at",       StringType(), False),
        StructField("row_count",         LongType(),   True),
        StructField("violation_count",   LongType(),   True),
    ])
    spark.createDataFrame(record, schema) \
         .write.format("delta").mode("append").save(METADATA_PATH)


def run_copper_pipeline():
    """
    Copper pipeline — validates per contract YAML, writes metadata on completion.
    This function is called from both Orchestrated and Autonomous Airflow DAGs.
    """
    run_seq = get_next_run_seq()

    try:
        # Read from Bronze
        df = spark.read.format("delta").load(BRONZE_PATH)

        # ── Schema enforcement per contract ──────────────────────
        from pyspark.sql.types import DecimalType
        df = df.withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))
        df = df.withColumn("transaction_date", F.col("transaction_date").cast("date"))

        # ── Reference and master data validation per contract ────
        valid_currencies = spark.table("copper.reference.iso_currencies") \
                               .select("code").rdd.flatMap(lambda x: x).collect()
        valid_units      = spark.table("copper.master.business_units") \
                               .select("code").rdd.flatMap(lambda x: x).collect()

        violations = df.filter(
            F.col("transaction_id").isNull() |
            F.col("amount").isNull() |
            ~F.col("currency").isin(valid_currencies) |
            ~F.col("business_unit").isin(valid_units) |
            ~F.col("recognition_policy").isin(["ASC606", "IFRS15"])
        )
        violation_count = violations.count()

        if violation_count > 0:
            # Contract FAILED — write status, halt pipeline
            write_contract_metadata(run_seq, "FAILED", df.count(), violation_count)
            raise ValueError(
                f"Contract '{CONTRACT_NAME}' v{CONTRACT_VERSION} FAILED. "
                f"{violation_count} violations found. Silver pipelines will not proceed."
            )

        # ── Classification-driven security ───────────────────────
        df = df.withColumn("amount",
            F.when(F.current_user().isin(["executive", "finance_analyst"]),
                   F.col("amount")).otherwise(F.lit(None))
        )

        # ── Write to Copper zone ─────────────────────────────────
        df.write.format("delta").mode("overwrite").save(COPPER_PATH)

        # Contract PASSED — write metadata record
        # This record is what both Finance (Orchestrated) and Sales (Autonomous) depend on
        write_contract_metadata(run_seq, "PASSED", df.count(), 0)
        print(f"Contract '{CONTRACT_NAME}' v{CONTRACT_VERSION} PASSED. run_seq={run_seq}")
        return run_seq

    except ValueError:
        raise
    except Exception as e:
        write_contract_metadata(run_seq, "FAILED", 0, -1)
        raise


# ═══════════════════════════════════════════════════════════════════
# PATTERN 1: ORCHESTRATED — Finance Silver (Airflow triggered)
# Copper DAG triggers Finance Silver DAG on successful contract validation
# Finance Silver trusts Copper — no re-validation, proceeds immediately
# ═══════════════════════════════════════════════════════════════════

# --- copper_dag.py (Airflow) ---
#
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from datetime import datetime, timedelta
#
# with DAG("copper_sales_pipeline",
#          schedule_interval="0 6 * * *",  # Daily at 06:00 UTC per contract SLA
#          catchup=False) as dag:
#
#     run_copper = PythonOperator(
#         task_id="run_copper_pipeline",
#         python_callable=run_copper_pipeline,
#     )
#
#     # Trigger Finance Silver immediately after Copper PASSED
#     # run_seq is passed via conf so Finance Silver knows which record to process
#     trigger_finance = TriggerDagRunOperator(
#         task_id="trigger_finance_silver",
#         trigger_dag_id="finance_silver_pipeline",
#         conf={"copper_run_seq": "{{ ti.xcom_pull(task_ids='run_copper_pipeline') }}"},
#         wait_for_completion=False,  # Fire and forget — Finance runs independently
#     )
#
#     run_copper >> trigger_finance


# --- finance_silver_dag.py (Airflow) ---
#
# with DAG("finance_silver_pipeline",
#          schedule_interval=None,  # Only triggered by Copper — never runs on its own
#          catchup=False) as dag:
#
#     def run_finance_silver(**context):
#         # Copper run_seq passed via DAG conf — no metadata table lookup needed
#         copper_run_seq = context["dag_run"].conf.get("copper_run_seq")
#
#         # Read Copper output — trust the contract, no re-validation
#         df_copper = spark.read.format("delta").load(COPPER_PATH)
#
#         # Finance domain transformation — fiscal quarter and inter-company flags
#         df_finance = df_copper.withColumn("fiscal_quarter",
#             F.concat(
#                 F.lit("Q"),
#                 F.ceil(F.month("transaction_date") / 3).cast("string"),
#                 F.lit("-FY"),
#                 F.year("transaction_date").cast("string")
#             )
#         ).withColumn("is_intercompany",
#             F.col("business_unit").isin(["HQ", "SHARED_SERVICES"])
#         )
#
#         df_finance.write.format("delta").mode("overwrite") \
#             .save("abfss://silver@your-storage-account.dfs.core.windows.net/finance/revenue")
#
#         # Update Finance last_processed_seq
#         spark.createDataFrame([{
#             "domain": "finance",
#             "source_table": TABLE_NAME,
#             "contract_name": CONTRACT_NAME,
#             "last_processed_seq": int(copper_run_seq),
#             "processed_at": datetime.utcnow().isoformat()
#         }]).write.format("delta").mode("overwrite") \
#             .save("abfss://silver@your-storage-account.dfs.core.windows.net/finance/_domain_metadata")
#
#         print(f"Finance Silver complete. copper_run_seq={copper_run_seq}")
#
#     run_finance = PythonOperator(
#         task_id="run_finance_silver",
#         python_callable=run_finance_silver,
#         provide_context=True,
#     )


# ═══════════════════════════════════════════════════════════════════
# PATTERN 2: AUTONOMOUS — Sales Silver (metadata table driven)
# Sales Silver runs on its own schedule, independent of Copper
# Checks _contract_metadata for new PASSED records using last_processed_seq
# No Airflow dependency on Copper DAG
# ═══════════════════════════════════════════════════════════════════

# --- sales_silver_dag.py (Airflow) ---
#
# with DAG("sales_silver_pipeline",
#          schedule_interval="*/30 * * * *",  # Every 30 min — Sales runs more frequently
#          catchup=False) as dag:
#
#     def run_sales_silver(**context):
#
#         # Step 1 — Get Sales domain's last processed seq from its own metadata
#         try:
#             sales_meta = spark.read.format("delta") \
#                 .load("abfss://silver@your-storage-account.dfs.core.windows.net/sales/_domain_metadata") \
#                 .filter("source_table = 'sales_transactions'") \
#                 .agg(F.max("last_processed_seq").alias("last_seq")) \
#                 .collect()[0]["last_seq"] or 0
#         except Exception:
#             sales_meta = 0  # First run
#
#         # Step 2 — Check Copper metadata for new PASSED records
#         # last_processed_seq in WHERE clause — no separate watermark lookup
#         new_copper = spark.read.format("delta").load(METADATA_PATH).filter(
#             (F.col("contract_name")    == CONTRACT_NAME) &
#             (F.col("contract_version") == CONTRACT_VERSION) &
#             (F.col("status")           == "PASSED") &
#             (F.col("run_seq")          >  sales_meta)   # Only unprocessed records
#         ).orderBy("run_seq").limit(1).collect()
#
#         if not new_copper:
#             print(f"No new Copper contract records since seq={sales_meta}. "
#                   f"Sales Silver will retry on next scheduled run.")
#             return  # Exit gracefully — retry on next schedule
#
#         copper_run_seq = new_copper[0]["run_seq"]
#         print(f"New Copper contract record found. run_seq={copper_run_seq}. Proceeding.")
#
#         # Step 3 — Read Copper output — trust the contract, no re-validation
#         df_copper = spark.read.format("delta").load(COPPER_PATH)
#
#         # Sales domain transformation — territory mapping and product categorization
#         df_sales = df_copper.withColumn("sales_territory",
#             F.when(F.col("region").isin(["US-East", "US-West"]), "North America")
#              .when(F.col("region").isin(["UK", "Germany"]), "EMEA")
#              .otherwise("APAC")
#         ).withColumn("product_category",
#             F.when(F.col("product") == "Enterprise Suite", "Enterprise")
#              .otherwise("Commercial")
#         )
#
#         df_sales.write.format("delta").mode("overwrite") \
#             .save("abfss://silver@your-storage-account.dfs.core.windows.net/sales/pipeline")
#
#         # Step 4 — Update Sales last_processed_seq
#         spark.createDataFrame([{
#             "domain": "sales",
#             "source_table": TABLE_NAME,
#             "contract_name": CONTRACT_NAME,
#             "last_processed_seq": copper_run_seq,
#             "processed_at": datetime.utcnow().isoformat()
#         }]).write.format("delta").mode("overwrite") \
#             .save("abfss://silver@your-storage-account.dfs.core.windows.net/sales/_domain_metadata")
#
#         print(f"Sales Silver complete. copper_run_seq={copper_run_seq}")
#
#     run_sales = PythonOperator(
#         task_id="run_sales_silver",
#         python_callable=run_sales_silver,
#         provide_context=True,
#     )
