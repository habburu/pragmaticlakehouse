-- ================================================================================
-- Pragmatic Lakehouse Architecture (PLA)
-- PLA Open Blueprint: PLA-OBP-002-v01
-- Title: Governing the Zone Boundaries
-- File: PLA-OBP-002-v01-sales-silver-macros.sql
-- ================================================================================
-- DISCLAIMER: This is illustrative skeleton code only. It is not production-ready.
-- ================================================================================
--
-- Two macros for the Sales Silver Autonomous pipeline:
--   1. check_copper_contract()    — pre-hook: checks _contract_metadata for PASSED
--   2. update_last_processed_seq() — post-hook: records successful run seq

-- ── Macro 1: Pre-hook — contract status check ─────────────────────────────────
-- Checks _contract_metadata for a new PASSED record since last run
-- If found   → returns run_seq, model proceeds
-- If not found → raises error, model stops gracefully, retries on next schedule

{% macro check_copper_contract() %}

    {% set contract_name    = "sales_transactions_copper_silver_data_contract" %}
    {% set contract_version = "1.0.0" %}
    {% set table_name       = "sales_transactions" %}

    -- Get Sales domain last_processed_seq
    {% set last_seq_result = run_query("
        SELECT COALESCE(MAX(last_processed_seq), 0) AS last_seq
        FROM silver.sales._domain_metadata
        WHERE source_table = '" ~ table_name ~ "'
    ") %}
    {% set last_seq = last_seq_result.rows[0][0] %}

    -- Check Copper metadata for new PASSED record
    {% set contract_check = run_query("
        SELECT run_seq
        FROM copper._contract_metadata
        WHERE contract_name    = '" ~ contract_name ~ "'
          AND contract_version = '" ~ contract_version ~ "'
          AND status           = 'PASSED'
          AND run_seq          > " ~ last_seq ~ "
        ORDER BY run_seq
        LIMIT 1
    ") %}

    {% if contract_check.rows | length == 0 %}
        -- No new PASSED record — exit gracefully
        {{ exceptions.raise_compiler_error(
            "No new contract records since seq=" ~ last_seq ~ ". "
            "Sales Silver will retry on next scheduled run."
        ) }}
    {% else %}
        -- New PASSED record found — store run_seq for post-hook
        {% set copper_run_seq = contract_check.rows[0][0] %}
        {{ log("Contract PASSED. copper_run_seq=" ~ copper_run_seq ~ ". Proceeding.", info=True) }}
    {% endif %}

{% endmacro %}


-- ── Macro 2: Post-hook — update last_processed_seq ───────────────────────────
-- Updates Sales domain metadata after successful model run
-- Records which Copper run_seq was processed

{% macro update_last_processed_seq() %}

    {% set contract_name = "sales_transactions_copper_silver_data_contract" %}
    {% set table_name    = "sales_transactions" %}

    -- Get the run_seq that was just processed
    {% set latest_seq = run_query("
        SELECT MAX(run_seq)
        FROM copper._contract_metadata
        WHERE contract_name = '" ~ contract_name ~ "'
          AND status        = 'PASSED'
    ") %}

    {% set copper_run_seq = latest_seq.rows[0][0] %}

    -- Update Sales domain metadata
    {% set update_sql %}
        MERGE INTO silver.sales._domain_metadata AS target
        USING (SELECT
            'sales'          AS domain,
            '{{ table_name }}'        AS source_table,
            '{{ contract_name }}'     AS contract_name,
            {{ copper_run_seq }}      AS last_processed_seq,
            CURRENT_TIMESTAMP         AS processed_at
        ) AS source
        ON target.source_table = source.source_table
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    {% endset %}

    {% do run_query(update_sql) %}
    {{ log("Sales last_processed_seq updated to " ~ copper_run_seq, info=True) }}

{% endmacro %}
