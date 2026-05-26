"""
================================================================================
Pragmatic Lakehouse Architecture (PLA)
PLA Open Blueprint: PLA-OBP-002-v01
Title: Governing the Zone Boundaries
File: PLA-OBP-002-v01-copper-airflow-dag.py
================================================================================
DISCLAIMER: This is illustrative skeleton code only. It is not production-ready.
================================================================================

Central team Airflow DAG:
  - Runs Copper enforcement pipeline daily per contract SLA
  - On success: triggers Finance Silver DAG (Orchestrated pattern)
  - Does NOT trigger Sales Silver — Sales is Autonomous
  - copper_run_seq is passed via conf so Finance Silver knows
    exactly which contract record it is processing
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

# Import Copper pipeline from enforcement module
from PLA_OBP_002_v01_copper_contract_enforcement import run_copper_pipeline

with DAG(
    dag_id="copper_sales_pipeline",
    schedule_interval="0 6 * * *",  # Daily at 06:00 UTC — per contract delivery_time
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["copper", "pla", "central-team"]
) as dag:

    run_copper = PythonOperator(
        task_id="run_copper_pipeline",
        python_callable=run_copper_pipeline,
        # Returns run_seq via XCom on success
        # Raises exception on FAILED — downstream trigger will not fire
    )

    # Trigger Finance Silver immediately after Copper PASSED
    # copper_run_seq passed via conf — Finance Silver uses this to identify
    # which _contract_metadata record it is processing
    trigger_finance_silver = TriggerDagRunOperator(
        task_id="trigger_finance_silver",
        trigger_dag_id="finance_silver_pipeline",
        conf={"copper_run_seq": "{{ ti.xcom_pull(task_ids='run_copper_pipeline') }}"},
        wait_for_completion=False,  # Fire and proceed — Finance runs independently
    )

    run_copper >> trigger_finance_silver
    # Sales Silver is NOT in this DAG — it runs Autonomously on its own schedule
