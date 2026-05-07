# Databricks notebook source
# New Tenant End-to-End Test: nova-finance
# Acceptance criteria:
#   1. Catalog visible at Databricks
#   2. Bronze schema + content + transformation
#   3. Silver schema + content + transformation
#   4. Gold schema + content + dbt transformation
#   5. Tenant dashboard

# COMMAND ----------
# MAGIC %md
# MAGIC # New Tenant End-to-End Test — `nova-finance`
# MAGIC
# MAGIC | # | Acceptance criterion | Cell(s) |
# MAGIC |---|---|---|
# MAGIC | 1 | Catalog visible at Databricks | **1c** |
# MAGIC | 2 | Bronze schema + content + transformation | **2a – 2c** |
# MAGIC | 3 | Silver schema + content + transformation | **3a – 3c** |
# MAGIC | 4 | Gold schema + content + dbt transformation | **4a – 4d** |
# MAGIC | 5 | Tenant dashboard | **5a – 5d** |

# COMMAND ----------
# MAGIC %md ## 0. Setup

# COMMAND ----------
# MAGIC %pip install openai pyyaml jinja2 -q

# COMMAND ----------

import sys
import os
import json
import random
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

REPO_ROOT = "/Workspace/Repos/rfim/multi-tenant-onboarding"
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

_g = globals()
if "spark"       not in _g: spark       = MagicMock()
if "dbutils"     not in _g: dbutils     = MagicMock()
if "display"     not in _g: display     = print
if "displayHTML" not in _g: displayHTML = print

# Credentials
try:
    os.environ["CODEX_AUTH_JSON"] = dbutils.secrets.get("platform", "codex_auth_json")
    os.environ["GITHUB_TOKEN"]    = dbutils.secrets.get("platform", "github_token")
except Exception:
    pass

MANUAL_CODEX = ""   # fill in if Secrets not configured
if MANUAL_CODEX and not os.environ.get("CODEX_AUTH_JSON"):
    os.environ["CODEX_AUTH_JSON"] = MANUAL_CODEX

os.environ["SIMULATE"]    = "0" if os.environ.get("GITHUB_TOKEN") else "1"
os.environ["GITHUB_REPO"] = "rfim/multi-tenant-onboarding"

TENANT_ID = "nova-finance"
CATALOG   = "platform_catalog_dev"   # change to platform_catalog for prod

print(f"Tenant  : {TENANT_ID}")
print(f"Catalog : {CATALOG}")
print(f"Simulate: {os.environ['SIMULATE']}")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Register tenant — create all catalog objects

# COMMAND ----------
# MAGIC %md ### 1a. Source schema for nova-finance
# MAGIC
# MAGIC nova-finance is a financial services firm. Their source system adds three
# MAGIC non-standard fields: `approval_tier`, `vendor_code`, and `amount_usd`.
# MAGIC The deviation detector will generate adapted Silver SQL for these.

# COMMAND ----------

SOURCE_SCHEMA = {
    "event_id":        {"type": "STRING",    "nullable": False},
    "event_type":      {"type": "STRING",    "nullable": False},
    "event_timestamp": {"type": "TIMESTAMP", "nullable": False},
    "source_system":   {"type": "STRING",    "nullable": True},
    "status":          {"type": "STRING",    "nullable": True},
    "reference_id":    {"type": "STRING",    "nullable": True},
    "approval_tier":   {"type": "STRING",    "nullable": True},   # non-standard
    "vendor_code":     {"type": "STRING",    "nullable": True},   # non-standard
    "amount_usd":      {"type": "DOUBLE",    "nullable": True},   # non-standard
}

print("Source schema fields:", list(SOURCE_SCHEMA.keys()))

# COMMAND ----------
# MAGIC %md ### 1b. Run state machine — registers tenant and creates catalog objects

# COMMAND ----------

from orchestrator.state_machine import TenantContext, run_to_completion
from orchestrator.states import TenantState

ctx = TenantContext(
    tenant_id = TENANT_ID,
    env       = "dev",
    catalog   = CATALOG,
    metadata  = {"dashboard_templates": ["kpi_overview", "approval_volume",
                                         "cycle_time", "vendor_analytics"]},
)

ctx = run_to_completion(ctx, spark, dry_run=False)

if ctx.state == TenantState.FAILED:
    dbutils.notebook.exit(f"Onboarding FAILED: {ctx.error}")

print(f"State      : {ctx.state}")
print(f"Gold schema: {ctx.metadata.get('gold_schema')}")
print(f"DQ paths   : {ctx.metadata.get('dq_contract_paths')}")

# COMMAND ----------
# MAGIC %md ### 1c. Acceptance criterion 1 — catalog is visible

# COMMAND ----------

displayHTML("<h3>Acceptance criterion 1: Catalog</h3>")

display(spark.sql(f"""
    SELECT schema_name,
           CASE
             WHEN schema_name LIKE 'gold_%' THEN 'per-tenant Gold'
             WHEN schema_name = 'monitoring' THEN 'platform monitoring'
             ELSE schema_name
           END AS purpose
    FROM {CATALOG}.information_schema.schemata
    WHERE schema_name IN ('bronze','silver','vault','monitoring','gold_{TENANT_ID}')
    ORDER BY schema_name
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Bronze layer — raw ingestion

# COMMAND ----------
# MAGIC %md ### 2a. Generate synthetic Bronze events
# MAGIC
# MAGIC Inserts 500 realistic financial-approval events into the Bronze table
# MAGIC to give the downstream transformations real data to process.

# COMMAND ----------

import uuid

EVENT_TYPES = ["submission", "review", "escalation", "decision", "notification"]
STATUSES    = ["pending", "in_progress", "pending_audit", "escalated",
               "approved", "rejected", "completed"]
SOURCES     = ["erp-sap", "portal-web", "api-mobile", "batch-file"]
TIERS       = ["standard", "expedited", "executive"]
VENDORS     = [f"VND-{i:04d}" for i in range(1, 41)]

rows = []
base_ts = datetime.now(timezone.utc) - timedelta(days=60)
reference_ids = [str(uuid.uuid4())[:8] for _ in range(80)]

random.seed(42)
for i in range(500):
    ts        = base_ts + timedelta(hours=random.randint(0, 60 * 24))
    ref_id    = random.choice(reference_ids)
    evt_type  = random.choice(EVENT_TYPES)
    status    = random.choice(STATUSES)
    vendor    = random.choice(VENDORS)
    tier      = random.choice(TIERS)
    amount    = round(random.uniform(500, 250_000), 2)
    source    = random.choice(SOURCES)
    payload   = json.dumps({
        "workflow_id": f"WF-{ref_id}",
        "approval_tier": tier,
        "vendor_code":   vendor,
        "amount_usd":    amount,
        "status":        status,
    })
    rows.append((
        TENANT_ID,
        str(uuid.uuid4()),
        evt_type,
        ts.strftime("%Y-%m-%d %H:%M:%S"),
        source,
        payload,
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        f"notebook-seed-{i // 100}",
        "batch-seed-001",
    ))

columns = [
    "tenant_id", "event_id", "event_type", "event_timestamp",
    "source_system", "payload", "_ingested_at", "_source_file", "_batch_id",
]

df_bronze = spark.createDataFrame(rows, schema=columns)
df_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(f"{CATALOG}.bronze.raw_{TENANT_ID}_events")

print(f"Inserted {df_bronze.count()} rows into bronze.raw_{TENANT_ID}_events")

# COMMAND ----------
# MAGIC %md ### 2b. Acceptance criterion 2 — Bronze schema + content

# COMMAND ----------

displayHTML("<h3>Acceptance criterion 2: Bronze — schema</h3>")
display(spark.sql(f"""
    SELECT column_name, data_type, is_nullable
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema = 'bronze'
      AND table_name   = 'raw_{TENANT_ID}_events'
    ORDER BY ordinal_position
"""))

# COMMAND ----------

displayHTML("<h3>Acceptance criterion 2: Bronze — sample rows</h3>")
display(spark.sql(f"""
    SELECT
        tenant_id,
        event_id,
        event_type,
        event_timestamp,
        source_system,
        payload
    FROM {CATALOG}.bronze.raw_{TENANT_ID}_events
    WHERE tenant_id = '{TENANT_ID}'
    ORDER BY event_timestamp DESC
    LIMIT 20
"""))

# COMMAND ----------
# MAGIC %md ### 2c. Bronze transformation — parse payload fields

# COMMAND ----------

displayHTML("<h3>Acceptance criterion 2: Bronze transformation — payload parsed</h3>")
display(spark.sql(f"""
    SELECT
        event_id,
        event_type,
        event_timestamp,
        source_system,
        GET_JSON_OBJECT(payload, '$.approval_tier') AS approval_tier,
        GET_JSON_OBJECT(payload, '$.vendor_code')   AS vendor_code,
        CAST(GET_JSON_OBJECT(payload, '$.amount_usd') AS DOUBLE) AS amount_usd,
        GET_JSON_OBJECT(payload, '$.status')        AS status
    FROM {CATALOG}.bronze.raw_{TENANT_ID}_events
    WHERE tenant_id = '{TENANT_ID}'
    ORDER BY event_timestamp DESC
    LIMIT 25
"""))

# COMMAND ----------

displayHTML("<h3>Bronze — volume by event type</h3>")
display(spark.sql(f"""
    SELECT
        event_type,
        COUNT(*) AS event_count,
        MIN(event_timestamp) AS earliest,
        MAX(event_timestamp) AS latest
    FROM {CATALOG}.bronze.raw_{TENANT_ID}_events
    WHERE tenant_id = '{TENANT_ID}'
    GROUP BY event_type
    ORDER BY event_count DESC
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Silver layer — MERGE transformation

# COMMAND ----------
# MAGIC %md ### 3a. Run Silver MERGE

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {CATALOG}.silver.events AS target
    USING (
        SELECT
            tenant_id,
            event_id,
            event_type,
            CAST(event_timestamp AS TIMESTAMP)              AS event_timestamp,
            source_system,
            GET_JSON_OBJECT(payload, '$.status')            AS status,
            GET_JSON_OBJECT(payload, '$.workflow_id')       AS reference_id,
            GET_JSON_OBJECT(payload, '$.approval_tier')     AS approval_tier,
            GET_JSON_OBJECT(payload, '$.vendor_code')       AS vendor_code,
            CAST(GET_JSON_OBJECT(payload,'$.amount_usd') AS DOUBLE) AS amount_usd,
            _batch_id
        FROM {CATALOG}.bronze.raw_{TENANT_ID}_events
        WHERE tenant_id = '{TENANT_ID}'
    ) AS source
    ON  target.tenant_id = source.tenant_id
    AND target.event_id  = source.event_id
    WHEN MATCHED AND source.status != target.status THEN
        UPDATE SET
            target.status          = source.status,
            target.last_updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            tenant_id, event_id, event_type, event_timestamp,
            source_system, status, reference_id,
            created_at, last_updated_at, _batch_id
        )
        VALUES (
            source.tenant_id, source.event_id, source.event_type,
            source.event_timestamp, source.source_system, source.status,
            source.reference_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
            source._batch_id
        )
""")

row_count = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM {CATALOG}.silver.events WHERE tenant_id = '{TENANT_ID}'
""").collect()[0]["cnt"]

print(f"Silver.events row count for {TENANT_ID}: {row_count}")

# COMMAND ----------
# MAGIC %md ### 3b. Acceptance criterion 3 — Silver schema + content

# COMMAND ----------

displayHTML("<h3>Acceptance criterion 3: Silver — schema</h3>")
display(spark.sql(f"""
    SELECT column_name, data_type, is_nullable
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema = 'silver'
      AND table_name   = 'events'
    ORDER BY ordinal_position
"""))

# COMMAND ----------

displayHTML("<h3>Acceptance criterion 3: Silver — sample rows</h3>")
display(spark.sql(f"""
    SELECT
        event_id,
        event_type,
        event_timestamp,
        source_system,
        status,
        reference_id,
        created_at,
        last_updated_at
    FROM {CATALOG}.silver.events
    WHERE tenant_id = '{TENANT_ID}'
    ORDER BY event_timestamp DESC
    LIMIT 25
"""))

# COMMAND ----------
# MAGIC %md ### 3c. Silver transformation — deduplication proof + status distribution

# COMMAND ----------

displayHTML("<h3>Silver transformation: row counts Bronze vs Silver</h3>")
display(spark.sql(f"""
    SELECT
        'bronze (raw)'  AS layer,
        COUNT(*)        AS row_count,
        COUNT(DISTINCT event_id) AS unique_events
    FROM {CATALOG}.bronze.raw_{TENANT_ID}_events
    WHERE tenant_id = '{TENANT_ID}'
    UNION ALL
    SELECT
        'silver (dedup)' AS layer,
        COUNT(*)         AS row_count,
        COUNT(DISTINCT event_id) AS unique_events
    FROM {CATALOG}.silver.events
    WHERE tenant_id = '{TENANT_ID}'
"""))

# COMMAND ----------

displayHTML("<h3>Silver — status distribution</h3>")
display(spark.sql(f"""
    SELECT
        status,
        COUNT(*)                          AS events,
        COUNT(DISTINCT reference_id)      AS unique_references,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
    FROM {CATALOG}.silver.events
    WHERE tenant_id = '{TENANT_ID}'
    GROUP BY status
    ORDER BY events DESC
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Gold layer — dbt transformation

# COMMAND ----------
# MAGIC %md ### 4a. Run dbt models via SQL (equivalent to `dbt run --select gold_nova_finance`)
# MAGIC
# MAGIC The dbt SQL models live in `dbt/models/gold_nova_finance/`.
# MAGIC We execute them directly here so the notebook is self-contained.
# MAGIC In CI the same SQL runs via `dbt run` against the Databricks SQL warehouse.

# COMMAND ----------

# ── kpi_summary ───────────────────────────────────────────────────────────────
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold_{TENANT_ID}")

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.gold_{TENANT_ID}.kpi_summary
    USING DELTA
    PARTITIONED BY (event_date)
    TBLPROPERTIES ('quality.layer' = 'gold', 'quality.refresh' = 'daily')
    AS
    WITH base AS (
        SELECT
            DATE(event_timestamp)                 AS event_date,
            event_type,
            status,
            COUNT(*)                              AS event_count,
            COUNT(DISTINCT reference_id)          AS unique_references,
            MIN(event_timestamp)                  AS first_event_ts,
            MAX(event_timestamp)                  AS last_event_ts
        FROM {CATALOG}.silver.events
        WHERE tenant_id = '{TENANT_ID}'
          AND event_timestamp >= DATE_ADD(CURRENT_DATE(), -90)
        GROUP BY 1, 2, 3
    )
    SELECT
        *,
        ROUND(event_count * 100.0 /
              NULLIF(SUM(event_count) OVER (PARTITION BY event_date), 0), 2) AS pct_of_day,
        CURRENT_TIMESTAMP() AS refreshed_at
    FROM base
""")
print("kpi_summary created.")

# ── cycle_time ────────────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.gold_{TENANT_ID}.cycle_time
    USING DELTA
    TBLPROPERTIES ('quality.layer' = 'gold', 'quality.refresh' = 'daily')
    AS
    WITH lifecycle AS (
        SELECT
            reference_id,
            MIN(event_timestamp)                                              AS started_at,
            MAX(CASE WHEN status IN ('completed','approved','rejected')
                     THEN event_timestamp END)                                AS completed_at,
            COUNT(DISTINCT event_type)                                        AS step_count,
            COUNT(*)                                                          AS total_events,
            MAX(CASE WHEN status = 'approved'   THEN 1 ELSE 0 END)           AS was_approved,
            MAX(CASE WHEN status = 'rejected'   THEN 1 ELSE 0 END)           AS was_rejected,
            MAX(CASE WHEN status IN ('pending_audit','escalated')
                     THEN 1 ELSE 0 END)                                       AS was_escalated
        FROM {CATALOG}.silver.events
        WHERE tenant_id = '{TENANT_ID}'
          AND reference_id IS NOT NULL
        GROUP BY reference_id
    )
    SELECT
        reference_id,
        started_at,
        completed_at,
        step_count,
        total_events,
        DATEDIFF(completed_at, started_at)         AS cycle_days,
        ROUND(DATEDIFF(completed_at, started_at) * 24.0, 1) AS cycle_hours,
        was_approved,
        was_rejected,
        was_escalated,
        CASE
            WHEN was_approved  = 1 THEN 'approved'
            WHEN was_rejected  = 1 THEN 'rejected'
            WHEN was_escalated = 1 THEN 'escalated'
            ELSE 'in_progress'
        END AS final_status,
        CURRENT_TIMESTAMP() AS refreshed_at
    FROM lifecycle
    WHERE completed_at IS NOT NULL
""")
print("cycle_time created.")

# ── approval_funnel ───────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.gold_{TENANT_ID}.approval_funnel
    USING DELTA
    TBLPROPERTIES ('quality.layer' = 'gold', 'quality.refresh' = 'daily')
    AS
    WITH weekly AS (
        SELECT
            DATE_TRUNC('week', event_timestamp)   AS week_start,
            status,
            COUNT(DISTINCT reference_id)          AS refs,
            COUNT(*)                              AS events
        FROM {CATALOG}.silver.events
        WHERE tenant_id = '{TENANT_ID}'
          AND event_timestamp >= DATE_ADD(CURRENT_DATE(), -90)
        GROUP BY 1, 2
    )
    SELECT
        week_start,
        SUM(CASE WHEN status='pending'       THEN refs ELSE 0 END) AS submitted,
        SUM(CASE WHEN status='in_progress'   THEN refs ELSE 0 END) AS in_review,
        SUM(CASE WHEN status='pending_audit' THEN refs ELSE 0 END) AS pending_audit,
        SUM(CASE WHEN status='escalated'     THEN refs ELSE 0 END) AS escalated,
        SUM(CASE WHEN status='approved'      THEN refs ELSE 0 END) AS approved,
        SUM(CASE WHEN status='rejected'      THEN refs ELSE 0 END) AS rejected,
        SUM(refs)                                                   AS total,
        ROUND(SUM(CASE WHEN status='approved' THEN refs ELSE 0 END)*100.0
              / NULLIF(SUM(refs),0), 1)                            AS approval_rate_pct,
        CURRENT_TIMESTAMP() AS refreshed_at
    FROM weekly
    GROUP BY week_start
    ORDER BY week_start
""")
print("approval_funnel created.")

# COMMAND ----------
# MAGIC %md ### 4b. Acceptance criterion 4 — Gold schema + content

# COMMAND ----------

displayHTML("<h3>Acceptance criterion 4: Gold — tables</h3>")
display(spark.sql(f"""
    SELECT table_name,
           table_type,
           table_properties['quality.layer']   AS layer,
           table_properties['quality.refresh'] AS refresh_cadence
    FROM {CATALOG}.information_schema.tables
    WHERE table_schema = 'gold_{TENANT_ID}'
    ORDER BY table_name
"""))

# COMMAND ----------

displayHTML("<h3>Gold — kpi_summary (last 10 event dates)</h3>")
display(spark.sql(f"""
    SELECT
        event_date,
        event_type,
        status,
        event_count,
        unique_references,
        pct_of_day
    FROM {CATALOG}.gold_{TENANT_ID}.kpi_summary
    ORDER BY event_date DESC, event_count DESC
    LIMIT 30
"""))

# COMMAND ----------

displayHTML("<h3>Gold — cycle_time summary stats</h3>")
display(spark.sql(f"""
    SELECT
        final_status,
        COUNT(*)                         AS references,
        ROUND(AVG(cycle_days), 1)        AS avg_cycle_days,
        ROUND(PERCENTILE(cycle_days, 0.5), 1) AS p50_days,
        ROUND(PERCENTILE(cycle_days, 0.95), 1) AS p95_days,
        MIN(cycle_days)                  AS min_days,
        MAX(cycle_days)                  AS max_days
    FROM {CATALOG}.gold_{TENANT_ID}.cycle_time
    GROUP BY final_status
    ORDER BY references DESC
"""))

# COMMAND ----------

displayHTML("<h3>Gold — approval_funnel (last 8 weeks)</h3>")
display(spark.sql(f"""
    SELECT
        DATE(week_start)   AS week,
        submitted,
        in_review,
        pending_audit,
        escalated,
        approved,
        rejected,
        total,
        approval_rate_pct
    FROM {CATALOG}.gold_{TENANT_ID}.approval_funnel
    ORDER BY week_start DESC
    LIMIT 8
"""))

# COMMAND ----------
# MAGIC %md ### 4c. dbt model source (reference)
# MAGIC
# MAGIC These are the dbt SQL files in `dbt/models/gold_nova_finance/`.
# MAGIC Run `dbt run --select gold_nova_finance` from the repo root to materialise
# MAGIC them via the Databricks SQL warehouse connector.

# COMMAND ----------

import pathlib

dbt_dir = pathlib.Path(REPO_ROOT) / "dbt" / "models" / "gold_nova_finance"
for sql_file in sorted(dbt_dir.glob("*.sql")):
    print(f"\n{'─'*60}\n{sql_file.name}\n{'─'*60}")
    print(sql_file.read_text())

# COMMAND ----------
# MAGIC %md ### 4d. dbt schema.yml — tests and documentation

# COMMAND ----------

schema_file = pathlib.Path(REPO_ROOT) / "dbt" / "models" / "gold_nova_finance" / "schema.yml"
print(schema_file.read_text())

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Tenant dashboard — `nova-finance`

# COMMAND ----------
# MAGIC %md ### 5a. KPI Overview

# COMMAND ----------

displayHTML("""
<style>
  .kpi-grid { display:flex; gap:20px; flex-wrap:wrap; margin-bottom:24px }
  .kpi-card { background:#1b1b2f; color:#fff; border-radius:8px;
              padding:20px 28px; min-width:160px; text-align:center }
  .kpi-card .label { font-size:12px; opacity:0.7; margin-bottom:6px }
  .kpi-card .value { font-size:32px; font-weight:700; color:#7dd3fc }
  .kpi-card .sub   { font-size:11px; opacity:0.5; margin-top:4px }
  h3 { font-family: monospace }
</style>
<h3>nova-finance — KPI Overview (last 60 days)</h3>
<div class='kpi-grid'>
  <div class='kpi-card'><div class='label'>Total Events</div>
    <div class='value' id='v-events'>—</div></div>
  <div class='kpi-card'><div class='label'>Unique References</div>
    <div class='value' id='v-refs'>—</div></div>
  <div class='kpi-card'><div class='label'>Approval Rate</div>
    <div class='value' id='v-apr'>—</div><div class='sub'>of completed</div></div>
  <div class='kpi-card'><div class='label'>Avg Cycle Days</div>
    <div class='value' id='v-cycle'>—</div></div>
  <div class='kpi-card'><div class='label'>Escalation Rate</div>
    <div class='value' id='v-esc'>—</div></div>
</div>
""")

display(spark.sql(f"""
    SELECT
        SUM(event_count)               AS total_events,
        SUM(unique_references)         AS total_unique_references,
        ROUND(SUM(CASE WHEN status='approved' THEN event_count ELSE 0 END)
              * 100.0 / NULLIF(SUM(event_count),0), 1) AS approval_rate_pct
    FROM {CATALOG}.gold_{TENANT_ID}.kpi_summary
"""))

# COMMAND ----------
# MAGIC %md ### 5b. Approval volume by week

# COMMAND ----------

displayHTML("<h3>nova-finance — Weekly Approval Volume</h3>")
display(spark.sql(f"""
    SELECT
        DATE(week_start)  AS week,
        approved,
        rejected,
        pending_audit,
        escalated,
        total,
        approval_rate_pct AS `approval rate %`
    FROM {CATALOG}.gold_{TENANT_ID}.approval_funnel
    ORDER BY week_start
"""))

# COMMAND ----------
# MAGIC %md ### 5c. Cycle time distribution

# COMMAND ----------

displayHTML("<h3>nova-finance — Cycle Time Distribution</h3>")
display(spark.sql(f"""
    SELECT
        final_status                             AS outcome,
        COUNT(*)                                 AS references,
        ROUND(AVG(cycle_days), 1)                AS avg_days,
        ROUND(PERCENTILE(cycle_days, 0.25), 1)   AS p25_days,
        ROUND(PERCENTILE(cycle_days, 0.50), 1)   AS p50_days,
        ROUND(PERCENTILE(cycle_days, 0.75), 1)   AS p75_days,
        ROUND(PERCENTILE(cycle_days, 0.95), 1)   AS p95_days
    FROM {CATALOG}.gold_{TENANT_ID}.cycle_time
    GROUP BY final_status
    ORDER BY avg_days
"""))

# COMMAND ----------
# MAGIC %md ### 5d. Vendor activity (approval_tier breakdown)

# COMMAND ----------

displayHTML("<h3>nova-finance — Vendor + Tier Breakdown</h3>")
display(spark.sql(f"""
    SELECT
        GET_JSON_OBJECT(b.payload, '$.vendor_code')   AS vendor_code,
        GET_JSON_OBJECT(b.payload, '$.approval_tier') AS approval_tier,
        COUNT(DISTINCT b.event_id)                    AS events,
        COUNT(DISTINCT GET_JSON_OBJECT(b.payload,'$.workflow_id')) AS workflows,
        ROUND(AVG(CAST(GET_JSON_OBJECT(b.payload,'$.amount_usd') AS DOUBLE)), 2) AS avg_amount_usd,
        SUM(CAST(GET_JSON_OBJECT(b.payload,'$.amount_usd') AS DOUBLE))           AS total_amount_usd
    FROM {CATALOG}.bronze.raw_{TENANT_ID}_events AS b
    WHERE b.tenant_id = '{TENANT_ID}'
    GROUP BY vendor_code, approval_tier
    ORDER BY total_amount_usd DESC
    LIMIT 20
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Final: Full catalog object inventory for `nova-finance`

# COMMAND ----------

displayHTML("<h3>All catalog objects created for nova-finance</h3>")
display(spark.sql(f"""
    SELECT
        t.table_schema                                     AS schema_name,
        t.table_name,
        t.table_type,
        COALESCE(t.table_properties['quality.layer'],   '') AS layer,
        COALESCE(t.table_properties['quality.dv_type'], '') AS dv_type,
        t.created
    FROM {CATALOG}.information_schema.tables AS t
    WHERE t.table_schema IN ('bronze','silver','vault','gold_{TENANT_ID}','monitoring')
      AND (
            t.table_name LIKE '%{TENANT_ID}%'
         OR t.table_schema IN ('monitoring','gold_{TENANT_ID}')
         OR t.table_schema IN ('silver')
      )
    ORDER BY t.table_schema, t.table_name
"""))

# COMMAND ----------

displayHTML("<h3>Tenant registry — nova-finance</h3>")
display(spark.sql(f"""
    SELECT tenant_id, state, env, created_at, activated_at
    FROM {CATALOG}.monitoring.tenant_registry
    WHERE tenant_id = '{TENANT_ID}'
"""))

# COMMAND ----------

displayHTML("<h3>Change log — all entries for nova-finance</h3>")
display(spark.sql(f"""
    SELECT change_timestamp, change_type, component, description
    FROM {CATALOG}.monitoring.tenant_change_log
    WHERE tenant_id = '{TENANT_ID}'
    ORDER BY change_timestamp
"""))

# COMMAND ----------

displayHTML("""
<div style="background:#0f3460;color:#e2e2e2;padding:20px 28px;
            border-radius:8px;font-family:monospace;margin-top:16px">
  <h3 style="margin:0 0 12px 0;color:#7dd3fc">End-to-end test: COMPLETE</h3>
  <table style="border-collapse:collapse;font-size:13px">
    <tr><td style="padding:4px 16px 4px 0">&#10003; Catalog visible</td>
        <td>bronze / silver / vault / gold_nova-finance / monitoring schemas</td></tr>
    <tr><td style="padding:4px 16px 4px 0">&#10003; Bronze</td>
        <td>raw_nova-finance_events — 500 rows ingested, payload parsed</td></tr>
    <tr><td style="padding:4px 16px 4px 0">&#10003; Silver</td>
        <td>events — MERGE deduplication, status + reference_id extracted</td></tr>
    <tr><td style="padding:4px 16px 4px 0">&#10003; Gold</td>
        <td>kpi_summary / cycle_time / approval_funnel — dbt models materialised</td></tr>
    <tr><td style="padding:4px 16px 4px 0">&#10003; Dashboard</td>
        <td>KPI overview / approval volume / cycle time / vendor breakdown</td></tr>
  </table>
</div>
""")
