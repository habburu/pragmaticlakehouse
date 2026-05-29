# ================================================================================
# Pragmatic Lakehouse Architecture (PLA)
# PLA Open Blueprint: PLA-OBP-001-v01
# Title: Virtualize Progressively — Not All at Once
# File: PLA-OBP-001-v01-virtualize-progressively.py
# ================================================================================
# DISCLAIMER: Illustrative samples only — not production-ready.
# Adapt to your platform, tooling, and tenant configuration.
# ================================================================================
#
# Three federation mechanisms in order of preference:
#   1. Shortcuts        — storage-level, zero data movement
#   2. Live federation  — protocol-level, query-time access
#   3. Governed replication — physical copy, treat as managed exception
#
# ================================================================================


# ════════════════════════════════════════════════════════════════════════════════
# 1. SHORTCUTS — storage-level federation, zero data movement
# ════════════════════════════════════════════════════════════════════════════════
#
# Example: Microsoft Fabric pointing to a Databricks-managed Delta table on
# ADLS Gen2. Fabric reads the Delta files directly. No data movement.
#
# The same pattern applies to Snowflake-managed Iceberg tables — Fabric reads
# them through a OneLake shortcut with automatic Iceberg-to-Delta metadata
# virtualization.
# ────────────────────────────────────────────────────────────────────────────────

# Configured in Fabric via the OneLake Shortcut UI or REST API
POST https://api.fabric.microsoft.com/v1/workspaces/{your-workspace-id}/items
{
  "displayName": "gold_company_a_sales",
  "type": "Shortcut",
  "definition": {
    "path": "Tables/sales_transactions",
    "target": {
      "type": "AdlsGen2",
      "location": "abfss://gold-company-a@your-storage-account.dfs.core.windows.net",
      "subpath": "sales_transactions"   # Points to Delta table created by Databricks
    }
  }
}


# ════════════════════════════════════════════════════════════════════════════════
# 2. LIVE FEDERATION — protocol-level, query-time access
# ════════════════════════════════════════════════════════════════════════════════
#
# Example: Fabric Platinum semantic model querying a Snowflake Gold table.
# Direct Query at query time — no shared storage, rows fetched on demand.
# The Platinum semantic model holds the metric definitions; Snowflake serves
# the rows when a dashboard or AI agent queries.
# ────────────────────────────────────────────────────────────────────────────────

# Connection definition for the Fabric semantic model (TMSL / XMLA)
{
  "createOrReplace": {
    "object": { "database": "platinum_enterprise_model" },
    "database": {
      "name": "platinum_enterprise_model",
      "model": {
        "dataSources": [{
          "name": "snowflake_domain_b_gold",
          "connectionString":
            "Provider=SNOWFLAKE;Server=your-account.snowflakecomputing.com;"
            "Database=GOLD_DB;Schema=SALES;Warehouse=PLATINUM_WH;Role=PLATINUM_READER"
        }],
        "tables": [{
          "name": "revenue_transactions",
          "partitions": [{
            "source": {
              "type": "query",
              "dataSource": "snowflake_domain_b_gold",
              "expression": "SELECT * FROM gold.sales.revenue_transactions"
            }
          }],
          "mode": "directQuery"   # Federation — Snowflake answers at query time
        }]
      }
    }
  }
}


# ════════════════════════════════════════════════════════════════════════════════
# 3. GOVERNED REPLICATION — managed exception, two paths
# ════════════════════════════════════════════════════════════════════════════════
#
# When neither shortcut nor live federation is technically possible, replication
# is the fallback. Two paths are common:
#   Option A — native mirroring where the platform supports it
#   Option B — an ETL pipeline that materializes a physical Delta table
#
# Either way a physical copy now exists. Treat as a managed exception, audit
# the scope, and plan to retire as platform capabilities mature.
# ────────────────────────────────────────────────────────────────────────────────

# ── Option A — Native mirroring: Snowflake → Fabric Mirrored Database ─────────
# Configured in Fabric via the Mirroring UI or REST API
# Continuous, near-real-time, zero pipeline code — but limited to what
# mirroring supports.

POST https://api.fabric.microsoft.com/v1/workspaces/{your-workspace-id}/mirroredDatabases
{
  "displayName": "domain_b_snowflake_mirror",
  "definition": {
    "sourceType": "Snowflake",
    "connection": {
      "snowflakeAccount": "your-account.snowflakecomputing.com",
      "database": "GOLD_DB",
      "schema": "SALES"
    },
    "tables": ["REVENUE_TRANSACTIONS"],   # Minimize scope — only what Platinum needs
    "replicationMode": "continuous"
  }
}


# ── Option B — ETL into Fabric: materialize a physical Delta table in OneLake ─
# Use when mirroring is not supported, or when transforms / joins / filters
# are needed. Pipeline can be Data Factory, a Spark notebook, or dbt on Fabric.

# Example: Spark notebook reading from Snowflake and writing to a Fabric lakehouse
df = (spark.read
      .format("snowflake")
      .options(**{
          "sfURL":       "your-account.snowflakecomputing.com",
          "sfDatabase":  "GOLD_DB",
          "sfSchema":    "SALES",
          "sfWarehouse": "PLATINUM_WH",
          "sfRole":      "PLATINUM_READER"
      })
      .option("dbtable", "revenue_transactions")
      .load())

# [IMPLEMENT] Optional transforms — projection, filtering, enrichment
# df = df.select("transaction_id", "amount", "currency", "transaction_date") ...

# Materialize as a physical Delta table inside OneLake
(df.write.format("delta")
     .mode("overwrite")
     .save("abfss://platinum@onelake.dfs.fabric.microsoft.com/replicated/revenue_transactions"))

# Note: a physical copy now exists in OneLake
# Document: why virtualization was not possible, scope, plan to retire
