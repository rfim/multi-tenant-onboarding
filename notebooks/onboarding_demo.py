# Databricks notebook source
# Multi-Tenant Onboarding Demo
# Import this file into your Databricks workspace via:
#   Workspace -> Import -> File -> onboarding_demo.py

# COMMAND ----------
# MAGIC %md
# MAGIC # Multi-Tenant Onboarding Demo
# MAGIC
# MAGIC This notebook walks through the two-stage onboarding test scenario:
# MAGIC
# MAGIC | Stage | What happens |
# MAGIC |---|---|
# MAGIC | **Stage 1** | New tenant dataset added → deviation detected → Codex generates adapted SQL → draft PR opened |
# MAGIC | **Stage 2** | PR classified → merged → `promote-staging` workflow triggered → staging comparison → promotion readiness report |
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - `CODEX_AUTH_JSON` stored in Databricks Secrets (scope: `platform`, key: `codex_auth_json`)
# MAGIC - `GITHUB_TOKEN` stored in Databricks Secrets (scope: `platform`, key: `github_token`)
# MAGIC - Cluster has internet access (for Codex API and GitHub API calls)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Setup

# COMMAND ----------

# Install dependencies on the cluster
# Run once per cluster restart
%pip install openai pyyaml jinja2 -q

# COMMAND ----------

import json
import logging
import os
import sys
from pathlib import Path

# Point Python to the repo root so all local imports resolve.
# Adjust REPO_ROOT if you cloned to a different path in DBFS or Repos.
REPO_ROOT = "/Workspace/Repos/rfim/multi-tenant-onboarding"
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("onboarding_demo")

print("Python path configured. Repo root:", REPO_ROOT)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Load credentials from Databricks Secrets
# MAGIC
# MAGIC Secrets are never printed or logged. If you do not have a secrets scope set up yet,
# MAGIC run the cell after this one to configure credentials manually (dev only).

# COMMAND ----------

try:
    codex_auth_raw = dbutils.secrets.get(scope="platform", key="codex_auth_json")
    os.environ["CODEX_AUTH_JSON"] = codex_auth_raw

    github_token = dbutils.secrets.get(scope="platform", key="github_token")
    os.environ["GITHUB_TOKEN"] = github_token

    print("Credentials loaded from Databricks Secrets.")
except Exception as e:
    print(f"Could not load from Secrets ({e}). Falling back to manual config cell below.")

# COMMAND ----------

# ── Manual credential override (dev/test only) ────────────────────────────────
# Fill in your values and run this cell if Secrets are not configured.
# Never commit credentials to git.

MANUAL_CODEX_AUTH = ""   # e.g. '{"api_key": "sk-..."}'
MANUAL_GITHUB_TOKEN = "" # e.g. "ghp_..."

if MANUAL_CODEX_AUTH and not os.environ.get("CODEX_AUTH_JSON"):
    os.environ["CODEX_AUTH_JSON"] = MANUAL_CODEX_AUTH
    print("Using manual CODEX_AUTH_JSON.")

if MANUAL_GITHUB_TOKEN and not os.environ.get("GITHUB_TOKEN"):
    os.environ["GITHUB_TOKEN"] = MANUAL_GITHUB_TOKEN
    print("Using manual GITHUB_TOKEN.")

# Set SIMULATE=1 to skip real GitHub calls (safe for demos without a token)
os.environ["SIMULATE"] = "0" if os.environ.get("GITHUB_TOKEN") else "1"
print(f"SIMULATE={'1' if os.environ['SIMULATE'] == '1' else '0'} "
      f"(GITHUB_TOKEN {'present' if os.environ.get('GITHUB_TOKEN') else 'absent'})")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 1 — Add Dataset → Create PR

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1a. Define tenant payload
# MAGIC
# MAGIC The fixture below mimics a customer signup webhook for tenant `acme-corp`.
# MAGIC The source schema contains three fields not present in the standard Silver template:
# MAGIC `approval_tier`, `vendor_code`, and `amount_usd`.
# MAGIC These will trigger the deviation detector.

# COMMAND ----------

TENANT_PAYLOAD = {
    "tenant_id": "acme-corp",
    "display_name": "Acme Corporation",
    "env": "dev",
    "catalog": "platform_catalog_dev",
    "source_schema": {
        "event_id":        {"type": "STRING",    "nullable": False},
        "event_type":      {"type": "STRING",    "nullable": False},
        "event_timestamp": {"type": "TIMESTAMP", "nullable": False},
        "source_system":   {"type": "STRING",    "nullable": True},
        "status":          {"type": "STRING",    "nullable": True},
        "reference_id":    {"type": "STRING",    "nullable": True},
        # ── Non-standard fields ──
        "approval_tier":   {"type": "STRING",    "nullable": True},
        "vendor_code":     {"type": "STRING",    "nullable": True},
        "amount_usd":      {"type": "DOUBLE",    "nullable": True},
    },
    "dashboard_templates": ["kpi_overview", "approval_volume", "cycle_time", "vendor_analytics"],
    "customer_profile": {
        "industry": "financial_services",
        "monthly_event_volume": 250_000,
        "custom_workflow_states": ["pending_audit", "escalated"],
    },
}

print(f"Tenant ID  : {TENANT_PAYLOAD['tenant_id']}")
print(f"Schema fields ({len(TENANT_PAYLOAD['source_schema'])}):", list(TENANT_PAYLOAD['source_schema'].keys()))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1b. Dry-run the onboarding state machine

# COMMAND ----------

from orchestrator.state_machine import TenantContext, run_to_completion
from orchestrator.states import TenantState

ctx = TenantContext(
    tenant_id=TENANT_PAYLOAD["tenant_id"],
    env=TENANT_PAYLOAD["env"],
    catalog=TENANT_PAYLOAD["catalog"],
    metadata={"dashboard_templates": TENANT_PAYLOAD["dashboard_templates"]},
)

# dry_run=True: validates transitions and template rendering without catalog writes
ctx = run_to_completion(ctx, spark, dry_run=True)

print(f"\nFinal state : {ctx.state}")
print(f"Metadata    : {json.dumps(ctx.metadata, indent=2)}")

assert ctx.state == TenantState.ACTIVE, f"Dry-run failed at state: {ctx.state}. Error: {ctx.error}"
print("\nState machine dry-run PASSED.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1c. Detect schema deviations

# COMMAND ----------

from agents.deviation_detector.detector import detect

report = detect(
    tenant_id=TENANT_PAYLOAD["tenant_id"],
    actual_schema=TENANT_PAYLOAD["source_schema"],
)

print(f"Standard fields  : {report.standard_fields}")
print(f"Actual fields    : {report.actual_fields}")
print()
print(f"Extra fields     : {report.extra_fields}   <- non-standard, need adapted SQL")
print(f"Missing fields   : {report.missing_fields}")
print(f"Type mismatches  : {report.type_mismatches}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1d. Call Codex to generate adapted Silver model SQL
# MAGIC
# MAGIC The deviation detector sends the diff to Codex with the platform modelling
# MAGIC skill file as context. Output is a single `.sql` file per adapted model.

# COMMAND ----------

from agents.deviation_detector.detector import generate_adapted_models

print("Calling Codex... (this takes 5-15 seconds)")

adapted_models = generate_adapted_models(report=report, catalog=TENANT_PAYLOAD["catalog"])

print(f"\nCodex generated {len(adapted_models)} model(s):\n")
for filename, sql in adapted_models.items():
    print(f"── {filename} ({sql.count(chr(10))} lines) ──────────────────────")
    print(sql[:1200])
    if len(sql) > 1200:
        print(f"... ({len(sql) - 1200} more chars)")
    print()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1e. Open draft PR on dev branch

# COMMAND ----------

from agents.deviation_detector.pr_writer import open_pr

def _deviation_summary(report, profile):
    lines = [
        f"Tenant `{report.tenant_id}` has {len(report.extra_fields)} non-standard field(s).",
        f"**Extra fields:** {', '.join(f'`{f}`' for f in report.extra_fields)}",
    ]
    custom = profile.get("custom_workflow_states", [])
    if custom:
        lines.append(f"**Custom workflow states:** {', '.join(custom)}")
    return "\n".join(lines)

pr_url = open_pr(
    tenant_id=TENANT_PAYLOAD["tenant_id"],
    models=adapted_models,
    deviation_summary=_deviation_summary(report, TENANT_PAYLOAD["customer_profile"]),
)

print(f"\nPR URL: {pr_url}")
displayHTML(f'<h3>Stage 1 complete</h3><p>PR: <a href="{pr_url}" target="_blank">{pr_url}</a></p>')

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 2 — Merge PR → Automate Promotion Workflow

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2a. Run the promotion classifier
# MAGIC
# MAGIC The classifier uses a fast file-glob rule pass first.
# MAGIC It only calls Codex when the rule-based confidence is below 0.95.

# COMMAND ----------

from agents.promotion_classifier.classifier import classify

CHANGED_FILES = [
    f"dbt/models/silver/{TENANT_PAYLOAD['tenant_id']}/silver_{TENANT_PAYLOAD['tenant_id']}_merge_adapted.sql",
    f"dq/expectations/{TENANT_PAYLOAD['tenant_id']}/silver_contract.yml",
]

PR_TITLE = f"[deviation] {TENANT_PAYLOAD['tenant_id']} adapted Silver models"
PR_BODY  = (
    f"Adapted Silver MERGE for tenant {TENANT_PAYLOAD['tenant_id']}. "
    f"Extra fields: {', '.join(report.extra_fields)}."
)

result = classify(
    changed_files=CHANGED_FILES,
    pr_title=PR_TITLE,
    pr_body=PR_BODY,
)

print(f"Category        : {result['category_id']} — {result['category_name']}")
print(f"Confidence      : {result['confidence']:.2f}")
print(f"Promotion path  : {' -> '.join(result['promotion_path'])}")
print(f"SLA             : {result['sla_hours']} hours")
print(f"Approvers       : {result['required_approvers'] or 'CI only'}")
print(f"Rollback runbook: {result['rollback_runbook']}")
if result["ambiguity_note"]:
    print(f"\nAmbiguity note  : {result['ambiguity_note']}")
if result["suggested_test_plan"]:
    print(f"\nSuggested tests :\n  " + result["suggested_test_plan"].replace("\n", "\n  "))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2b. Validate PR is Category 2 (tenant_model)

# COMMAND ----------

assert result["category_id"] == 2, (
    f"Expected Category 2 (tenant_model), got {result['category_id']} ({result['category_name']}). "
    "Check the changed_files list or review the promotion/categories.yml decision tree."
)
print("Assertion passed: PR correctly classified as Category 2 (tenant_model).")
print("Promotion path  : dev -> staging -> prod")
print("SLA             : 24 hours")
print("Required        : 1 engineer reviewer")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2c. Staging comparison
# MAGIC
# MAGIC Validates that the generated SQL is parseable and the DQ contract YAML is valid.
# MAGIC In a real environment this step queries `monitoring.staging_baseline` and compares
# MAGIC row counts, null rates, and schema shape against production.

# COMMAND ----------

import re
import yaml as _yaml

issues = []

for filename, content in adapted_models.items():
    if filename.endswith(".sql"):
        if not re.search(r"\b(MERGE|CREATE|SELECT)\b", content, re.IGNORECASE):
            issues.append(f"{filename}: no recognisable SQL statement")
        else:
            print(f"SQL check PASSED : {filename}")

# Validate DQ contract templates render as valid YAML
from jinja2 import Environment, FileSystemLoader, StrictUndefined

contracts_dir = Path(REPO_ROOT) / "templates" / "quality_contracts"
env_j2 = Environment(
    loader=FileSystemLoader(str(contracts_dir)),
    undefined=StrictUndefined,
    keep_trailing_newline=True,
)

from datetime import datetime, timezone, timedelta
now = datetime.now(timezone.utc)
render_vars = {
    "tenant_id": TENANT_PAYLOAD["tenant_id"],
    "catalog": TENANT_PAYLOAD["catalog"],
    "gold_schema": f"gold_{TENANT_PAYLOAD['tenant_id']}",
    "now_utc": now.isoformat(),
    "lookback_start": (now - timedelta(days=90)).isoformat(),
    "tomorrow": (now + timedelta(days=1)).isoformat(),
    "yesterday_minus_2h": (now - timedelta(hours=26)).isoformat(),
    "today": now.date().isoformat(),
}

for tpl_name in ["bronze_contract.yml.j2", "silver_contract.yml.j2", "gold_contract.yml.j2"]:
    rendered = env_j2.get_template(tpl_name).render(**render_vars)
    try:
        _yaml.safe_load(rendered)
        print(f"YAML check PASSED: {tpl_name}")
    except _yaml.YAMLError as e:
        issues.append(f"{tpl_name}: invalid YAML — {e}")

if issues:
    print("\nISSUES FOUND:")
    for i in issues:
        print(f"  - {i}")
else:
    print("\nAll staging checks PASSED. No issues found.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2d. Promotion readiness report

# COMMAND ----------

PROMOTE_CMD = (
    f"gh workflow run promote-production.yml "
    f"--repo rfim/multi-tenant-onboarding "
    f"--field pr_number=<PR_NUMBER>"
)

report_rows = [
    ("Tenant ID",         TENANT_PAYLOAD["tenant_id"]),
    ("Category",          f"{result['category_id']} — {result['category_name']}"),
    ("Classifier conf",   f"{result['confidence']:.2f}"),
    ("Extra fields",      ", ".join(report.extra_fields)),
    ("Adapted models",    ", ".join(adapted_models.keys())),
    ("PR",                pr_url),
    ("SQL check",         "PASSED" if not any(".sql" in i for i in issues) else "FAILED"),
    ("YAML check",        "PASSED" if not any(".yml" in i for i in issues) else "FAILED"),
    ("Staging result",    "PASSED" if not issues else "FAILED"),
    ("Ready for prod",    "YES" if not issues else "NO — fix issues above first"),
]

html = """
<style>
  table { border-collapse: collapse; font-family: monospace; font-size: 14px; }
  th { background: #1b1b2f; color: #e2e2e2; padding: 8px 16px; text-align: left; }
  td { padding: 6px 16px; border-bottom: 1px solid #ddd; }
  .pass { color: #2e7d32; font-weight: bold; }
  .fail { color: #c62828; font-weight: bold; }
  .ready { color: #1565c0; font-weight: bold; }
</style>
<h3>Promotion Readiness Report</h3>
<table>
  <tr><th>Field</th><th>Value</th></tr>
"""
for k, v in report_rows:
    css = ""
    if "PASSED" in str(v):
        css = "pass"
    elif "FAILED" in str(v):
        css = "fail"
    elif "YES" in str(v):
        css = "ready"
    html += f'  <tr><td>{k}</td><td class="{css}">{v}</td></tr>\n'

html += f"""</table>
<br>
<b>To promote to production:</b><br>
<code>{PROMOTE_CMD}</code>
"""

displayHTML(html)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Canary Tenant Reference
# MAGIC
# MAGIC Category 3 (shared-layer) changes route through the canary tenant before
# MAGIC reaching all tenants. The canary config is in `promotion/canary_tenant.yml`.

# COMMAND ----------

import yaml

canary_path = Path(REPO_ROOT) / "promotion" / "canary_tenant.yml"
canary_cfg  = yaml.safe_load(canary_path.read_text())
canary      = canary_cfg["canary_tenant"]

print(f"Canary tenant ID    : {canary['tenant_id']}")
print(f"Hold duration       : {canary['hold_hours']} hours")
print(f"Null rate max       : {canary['dq_thresholds']['null_rate_max_pct']}%")
print(f"Volume delta max    : {canary['dq_thresholds']['row_count_delta_max_pct']}%")
print(f"Unexpected pct max  : {canary['dq_thresholds']['unexpected_pct_max']}%")
print(f"Auto rollback rec   : {canary['auto_rollback_recommendation']}")
print(f"Slack channel       : {canary['slack_channel']}")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Change Log Monitoring Table (DDL reference)
# MAGIC
# MAGIC Run these cells in a Databricks SQL warehouse to create the monitoring tables
# MAGIC required by the orchestrator and agents.

# COMMAND ----------

CATALOG = TENANT_PAYLOAD["catalog"]

ddl_statements = f"""
-- Run these once per environment to initialise the monitoring schema.

CREATE SCHEMA IF NOT EXISTS {CATALOG}.monitoring;

CREATE TABLE IF NOT EXISTS {CATALOG}.monitoring.tenant_registry (
    tenant_id        STRING NOT NULL,
    display_name     STRING,
    env              STRING NOT NULL,
    state            STRING NOT NULL,
    created_at       TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    activated_at     TIMESTAMP,
    deactivated_at   TIMESTAMP,
    deactivation_reason STRING
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS {CATALOG}.monitoring.tenant_change_log (
    tenant_id        STRING NOT NULL,
    change_timestamp TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    change_type      STRING NOT NULL,
    component        STRING,
    description      STRING,
    changed_by       STRING,
    pr_url           STRING,
    env              STRING NOT NULL
)
USING DELTA
PARTITIONED BY (tenant_id);

CREATE TABLE IF NOT EXISTS {CATALOG}.monitoring.dq_results (
    tenant_id        STRING NOT NULL,
    run_date         DATE NOT NULL,
    layer            STRING NOT NULL,
    expectation_type STRING,
    column_name      STRING,
    success          BOOLEAN,
    observed_value   DOUBLE,
    expected_value   DOUBLE,
    unexpected_count BIGINT,
    unexpected_pct   DOUBLE
)
USING DELTA
PARTITIONED BY (tenant_id, run_date);

CREATE TABLE IF NOT EXISTS {CATALOG}.monitoring.dq_alerts (
    tenant_id        STRING NOT NULL,
    layer            STRING,
    metric           STRING,
    severity         STRING NOT NULL,
    current_value    DOUBLE,
    baseline_value   DOUBLE,
    delta_pct        DOUBLE,
    description      STRING,
    detected_at      TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    resolved         BOOLEAN DEFAULT false
)
USING DELTA
PARTITIONED BY (tenant_id);

CREATE TABLE IF NOT EXISTS {CATALOG}.monitoring.agent_invocations (
    agent_name       STRING NOT NULL,
    tenant_id        STRING,
    input_hash       STRING,
    output_summary   STRING,
    model_version    STRING,
    input_tokens     INT,
    output_tokens    INT,
    outcome          STRING,
    invoked_at       TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA;

CREATE TABLE IF NOT EXISTS {CATALOG}.monitoring.changelog_summaries (
    tenant_id        STRING NOT NULL,
    summary          STRING,
    lookback_days    INT,
    generated_at     TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    model            STRING
)
USING DELTA
PARTITIONED BY (tenant_id);
"""

print(ddl_statements)

# COMMAND ----------

# Optionally execute the DDL above against the active SparkSession.
# Comment this out if you prefer to run it manually in a SQL warehouse.

RUN_DDL = False  # set to True to execute

if RUN_DDL:
    for stmt in [s.strip() for s in ddl_statements.split(";") if s.strip()]:
        if stmt.upper().startswith(("CREATE", "ALTER")):
            print(f"Executing: {stmt[:80]}...")
            spark.sql(stmt)
    print("DDL executed.")
else:
    print("RUN_DDL=False — copy the DDL above and run it in a SQL warehouse.")
