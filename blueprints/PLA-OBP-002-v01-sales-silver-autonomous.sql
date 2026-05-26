-- ================================================================================
-- Pragmatic Lakehouse Architecture (PLA)
-- PLA Open Blueprint: PLA-OBP-002-v01
-- Title: Governing the Zone Boundaries
-- File: PLA-OBP-002-v01-sales-silver-autonomous.sql
-- ================================================================================
-- DISCLAIMER: This is illustrative skeleton code only. It is not production-ready.
-- ================================================================================
--
-- Sales domain — Autonomous pipeline pattern using dbt
-- Runs on its own schedule — independent of Copper DAG
-- pre-hook:  checks _contract_metadata for a new PASSED record
--            exits gracefully if no new record found
-- model:     reads from copper.sales_transactions
-- post-hook: records the processed run sequence


-- ── Macro: pre-hook — contract status check ──────────────────────
{% macro check_copper_contract() %}
    {% set contract_name    = "sales_transactions_copper_silver_data_contract" %}
    {% set contract_version = "1.0.0" %}
    {% set table_name       = "sales_transactions" %}

    -- Get Sales domain last_processed_seq
    {% set last_seq = run_query("
        SELECT COALESCE(MAX(last_processed_seq), 0)
        FROM silver.sales._domain_metadata
        WHERE source_table = '" ~ table_name ~ "'
    ").rows[0][0] %}

    -- Check for new PASSED record since last run
    {% set result = run_query("
        SELECT run_seq FROM copper._contract_metadata
        WHERE contract_name    = '" ~ contract_name ~ "'
          AND contract_version = '" ~ contract_version ~ "'
          AND status           = 'PASSED'
          AND run_seq          > " ~ last_seq ~ "
        ORDER BY run_seq LIMIT 1
    ") %}

    {% if result.rows | length == 0 %}
        {{ exceptions.raise_compiler_error(
            "No new contract records since seq=" ~ last_seq ~ ". "
            "Sales Silver will retry on next scheduled run."
        ) }}
    {% else %}
        {{ log("Contract PASSED. run_seq=" ~ result.rows[0][0] ~ ". Proceeding.", info=True) }}
    {% endif %}
{% endmacro %}


-- ── Macro: post-hook — update last_processed_seq ─────────────────
{% macro update_last_processed_seq() %}
    {% set contract_name = "sales_transactions_copper_silver_data_contract" %}
    {% set table_name    = "sales_transactions" %}
    {% set copper_run_seq = run_query("
        SELECT MAX(run_seq) FROM copper._contract_metadata
        WHERE contract_name = '" ~ contract_name ~ "' AND status = 'PASSED'
    ").rows[0][0] %}

    {% do run_query("
        MERGE INTO silver.sales._domain_metadata AS target
        USING (SELECT 'sales' AS domain,
                      '" ~ table_name ~ "' AS source_table,
                      '" ~ contract_name ~ "' AS contract_name,
                      " ~ copper_run_seq ~ " AS last_processed_seq,
                      CURRENT_TIMESTAMP AS processed_at) AS source
        ON target.source_table = source.source_table
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ") %}
    {{ log("Sales last_processed_seq updated to " ~ copper_run_seq, info=True) }}
{% endmacro %}


-- ── dbt model ─────────────────────────────────────────────────────
{{ config(
    materialized = "table",
    pre_hook     = "{{ check_copper_contract() }}",
    post_hook    = "{{ update_last_processed_seq() }}"
) }}

SELECT *
FROM {{ source("copper", "sales_transactions") }}
-- [IMPLEMENT] Add Sales domain transformations here
