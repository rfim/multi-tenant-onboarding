# Databricks notebook source
# Multi-Tenant Onboarding Demo
# Import: Workspace -> Import -> File -> onboarding_demo.py

# COMMAND ----------
# MAGIC %md
# MAGIC # Multi-Tenant Onboarding Demo
# MAGIC
# MAGIC **Two stages — runs against a real Unity Catalog.**
# MAGIC
# MAGIC | Stage | What happens |
# MAGIC |---|---|
# MAGIC | **Stage 1** | Tenant registered → catalog objects created → DQ contracts written → tenant marked ACTIVE |
# MAGIC | **Stage 2** | Schema deviation detected → Codex generates adapted Silver SQL → draft PR opened → promotion classifier runs |
# MAGIC
# MAGIC **Prerequisites**
# MAGIC - Unity Catalog enabled workspace
# MAGIC - `CODEX_AUTH_JSON` in Databricks Secrets (`scope=platform, key=codex_auth_json`)
# MAGIC - Cluster runtime 13.3 LTS or higher, single-user mode

# COMMAND ----------
# MAGIC %md ## 0. Setup

# COMMAND ----------
# MAGIC %pip install openai pyyaml jinja2 -q

# COMMAND ----------

import sys
import os
import json
from unittest.mock import MagicMock

# Stubs first so dbutils is defined before any use below
_g = globals()
if "spark"       not in _g: spark       = MagicMock()
if "dbutils"     not in _g: dbutils     = MagicMock()
if "display"     not in _g: display     = print
if "displayHTML" not in _g: displayHTML = print

# Resolve repo root from the running notebook's workspace path — works for
# both DAB deployments (/Users/.../.bundle/.../files/) and Repos.
try:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    REPO_ROOT = "/Workspace" + "/".join(_nb_path.split("/")[:-2])
except Exception:
    REPO_ROOT = "/Workspace/Repos/rfim/multi-tenant-onboarding"

if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# Credentials from Databricks Secrets
try:
    os.environ["CODEX_AUTH_JSON"] = dbutils.secrets.get("platform", "codex_auth_json")
    os.environ["GITHUB_TOKEN"]    = dbutils.secrets.get("platform", "github_token")
    print("Credentials loaded from Secrets.")
except Exception:
    pass  # fall through to manual cell

# COMMAND ----------

# Manual override — fill in if Secrets scope is not configured
MANUAL_CODEX  = ""   # '{"api_key": "sk-..."}'
MANUAL_GITHUB = ""   # "ghp_..."

if MANUAL_CODEX  and not os.environ.get("CODEX_AUTH_JSON"):
    os.environ["CODEX_AUTH_JSON"] = MANUAL_CODEX
if MANUAL_GITHUB and not os.environ.get("GITHUB_TOKEN"):
    os.environ["GITHUB_TOKEN"] = MANUAL_GITHUB

# SIMULATE=1 skips real GitHub calls (safe without a token)
os.environ["SIMULATE"] = "0" if os.environ.get("GITHUB_TOKEN") else "1"
os.environ["GITHUB_REPO"] = "rfim/multi-tenant-onboarding"

print(f"CODEX_AUTH_JSON : {'SET' if os.environ.get('CODEX_AUTH_JSON') else 'MISSING'}")
print(f"GITHUB_TOKEN    : {'SET' if os.environ.get('GITHUB_TOKEN')    else 'MISSING (SIMULATE=1)'}")
print(f"SIMULATE        : {os.environ['SIMULATE']}")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 1 — Register tenant and create all catalog objects

# COMMAND ----------
# MAGIC %md ### 1a. Tenant payload

# COMMAND ----------

# Read parameters passed by the DAB job (or fall back to defaults for interactive use)
try:
    dbutils.widgets.text("catalog",   "platform_catalog_dev")
    dbutils.widgets.text("env",       "dev")
    dbutils.widgets.text("tenant_id", "acme-corp")
    CATALOG   = dbutils.widgets.get("catalog")
    ENV       = dbutils.widgets.get("env")
    _TENANT   = dbutils.widgets.get("tenant_id")
except Exception:
    CATALOG   = "platform_catalog_dev"
    ENV       = "dev"
    _TENANT   = "acme-corp"

print(f"CATALOG   : {CATALOG}")
print(f"ENV       : {ENV}")
print(f"TENANT_ID : {_TENANT}")

TENANT_PAYLOAD = {
    "tenant_id": _TENANT,
    "env":       ENV,
    "catalog":   CATALOG,
    "source_schema": {
        "event_id":        {"type": "STRING",    "nullable": False},
        "event_type":      {"type": "STRING",    "nullable": False},
        "event_timestamp": {"type": "TIMESTAMP", "nullable": False},
        "source_system":   {"type": "STRING",    "nullable": True},
        "status":          {"type": "STRING",    "nullable": True},
        "reference_id":    {"type": "STRING",    "nullable": True},
        "approval_tier":   {"type": "STRING",    "nullable": True},   # non-standard
        "vendor_code":     {"type": "STRING",    "nullable": True},   # non-standard
        "amount_usd":      {"type": "DOUBLE",    "nullable": True},   # non-standard
    },
    "dashboard_templates": ["kpi_overview", "approval_volume", "cycle_time"],
    "customer_profile": {
        "custom_workflow_states": ["pending_audit", "escalated"],
    },
}
print(f"Tenant : {TENANT_PAYLOAD['tenant_id']}")
print(f"Catalog: {CATALOG}")

# COMMAND ----------
# MAGIC %md ### 1b. Run the onboarding state machine (dry_run=False — real catalog writes)

# COMMAND ----------

from orchestrator.state_machine import TenantContext, run_to_completion
from orchestrator.states import TenantState

ctx = TenantContext(
    tenant_id = TENANT_PAYLOAD["tenant_id"],
    env       = TENANT_PAYLOAD["env"],
    catalog   = TENANT_PAYLOAD["catalog"],
    metadata  = {"dashboard_templates": TENANT_PAYLOAD["dashboard_templates"]},
)

ctx = run_to_completion(ctx, spark, dry_run=False)

print(f"Final state : {ctx.state}")
if ctx.state == TenantState.FAILED:
    print(f"Error       : {ctx.error}")
    raise RuntimeError(f"Onboarding failed: {ctx.error}")

print(f"Metadata    :\n{json.dumps(ctx.metadata, indent=2)}")

# COMMAND ----------
# MAGIC %md ### 1c. Verify — catalog objects created

# COMMAND ----------

tid      = TENANT_PAYLOAD["tenant_id"]
safe_tid = tid.replace("-", "_")   # safe for SQL identifiers

print("=" * 55)
print("SCHEMAS CREATED")
print("=" * 55)
display(spark.sql(f"""
    SELECT schema_name, created
    FROM {CATALOG}.information_schema.schemata
    WHERE schema_name IN ('bronze','silver','vault','gold_{safe_tid}','monitoring')
    ORDER BY schema_name
"""))

# COMMAND ----------

print("=" * 55)
print("TABLES CREATED")
print("=" * 55)
display(spark.sql(f"""
    SELECT table_schema, table_name, table_type, created
    FROM {CATALOG}.information_schema.tables
    WHERE table_schema IN ('bronze','silver','vault','gold_{safe_tid}','monitoring')
    ORDER BY table_schema, table_name
"""))

# COMMAND ----------

print("=" * 55)
print("DATA VAULT TABLES")
print("=" * 55)
display(spark.sql(f"""
    SELECT table_schema, table_name, table_type
    FROM {CATALOG}.information_schema.tables
    WHERE table_schema = 'vault'
      AND table_name LIKE '%{safe_tid}%'
    ORDER BY table_name
"""))

# COMMAND ----------

print("=" * 55)
print("TENANT REGISTRY")
print("=" * 55)
display(spark.sql(f"""
    SELECT tenant_id, state, env, created_at, activated_at
    FROM {CATALOG}.monitoring.tenant_registry
    WHERE tenant_id = '{tid}'
"""))

# COMMAND ----------

print("=" * 55)
print("DQ CONTRACTS REGISTERED")
print("=" * 55)
display(spark.sql(f"""
    SELECT tenant_id, layer, contract_path, is_active, created_at
    FROM {CATALOG}.monitoring.dq_contracts
    WHERE tenant_id = '{tid}'
    ORDER BY layer
"""))

# COMMAND ----------

print("=" * 55)
print("CHANGE LOG — last 20 entries for this tenant")
print("=" * 55)
display(spark.sql(f"""
    SELECT change_timestamp, change_type, component, description
    FROM {CATALOG}.monitoring.tenant_change_log
    WHERE tenant_id = '{tid}'
    ORDER BY change_timestamp DESC
    LIMIT 20
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 2 — Detect deviation → Codex generates adapted SQL → PR + classifier

# COMMAND ----------
# MAGIC %md ### 2a. Detect schema deviation

# COMMAND ----------

from agents.deviation_detector.detector import detect

report = detect(
    tenant_id     = TENANT_PAYLOAD["tenant_id"],
    actual_schema = TENANT_PAYLOAD["source_schema"],
)

print(f"Extra fields (non-standard) : {report.extra_fields}")
print(f"Missing fields              : {report.missing_fields}")
print(f"Type mismatches             : {report.type_mismatches}")

# COMMAND ----------
# MAGIC %md ### 2b. Generate adapted Silver SQL via Codex

# COMMAND ----------

from agents.deviation_detector.detector import generate_adapted_models

print("Calling Codex...")
adapted_models = generate_adapted_models(report=report, catalog=CATALOG)

for fname, sql in adapted_models.items():
    print(f"\n{'─'*55}\n{fname}  ({sql.count(chr(10))} lines)\n{'─'*55}")
    print(sql[:2000])
    if len(sql) > 2000:
        print(f"... +{len(sql)-2000} chars")

# COMMAND ----------
# MAGIC %md ### 2c. Open draft PR

# COMMAND ----------

from agents.deviation_detector.pr_writer import open_pr

deviation_summary = (
    f"{len(report.extra_fields)} non-standard field(s): "
    + ", ".join(f"`{f}`" for f in report.extra_fields)
)

pr_url = open_pr(
    tenant_id         = TENANT_PAYLOAD["tenant_id"],
    models            = adapted_models,
    deviation_summary = deviation_summary,
)

print(f"PR URL: {pr_url}")
displayHTML(f'<b>Draft PR:</b> <a href="{pr_url}" target="_blank">{pr_url}</a>')

# COMMAND ----------
# MAGIC %md ### 2d. Run promotion classifier against the PR

# COMMAND ----------

from agents.promotion_classifier.classifier import classify

result = classify(
    changed_files = [
        f"dbt/models/silver/{tid}/silver_{tid}_merge_adapted.sql",
        f"dq/expectations/{tid}/silver_contract.yml",
    ],
    pr_title = f"[deviation] {tid} adapted Silver models",
    pr_body  = f"Extra fields: {', '.join(report.extra_fields)}",
)

rows = [
    ("Category",       f"{result['category_id']} — {result['category_name']}"),
    ("Confidence",     f"{result['confidence']:.2f}"),
    ("Promotion path", " → ".join(result['promotion_path'])),
    ("SLA",            f"{result['sla_hours']}h"),
    ("Approvers",      str(result['required_approvers'] or 'CI only')),
    ("Rollback",       result.get('rollback_runbook', '')),
]

html = """<style>
  table{border-collapse:collapse;font-family:monospace;font-size:13px}
  th{background:#1b1b2f;color:#e2e2e2;padding:6px 14px;text-align:left}
  td{padding:5px 14px;border-bottom:1px solid #ddd}
</style><h3>Promotion classifier result</h3><table>
<tr><th>Field</th><th>Value</th></tr>"""
for k, v in rows:
    html += f"<tr><td>{k}</td><td>{v}</td></tr>"
html += "</table>"

if result.get("suggested_test_plan"):
    plan_html = result["suggested_test_plan"].replace("\n", "<br>")
    html += f"<br><b>Suggested test plan:</b><br>{plan_html}"

displayHTML(html)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 1 summary — all objects at a glance

# COMMAND ----------

all_tables = spark.sql(f"""
    SELECT table_schema AS schema,
           table_name,
           table_type,
           created
    FROM {CATALOG}.information_schema.tables
    WHERE table_schema IN ('bronze','silver','vault','gold_{safe_tid}','monitoring')
    ORDER BY table_schema, table_name
""")

print(f"Total objects created for catalog '{CATALOG}':")
display(all_tables)
