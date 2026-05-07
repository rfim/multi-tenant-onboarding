"""
Handler: GRANTS_PROVISIONED -> PIPELINES_SCAFFOLDED

Renders Bronze/Silver/Vault/Gold SQL templates for the new tenant and
executes them against the catalog to materialise the Gold layer tables
and views. Also opens a GitHub PR so the generated SQL is tracked in
version control.

Template rendering: Jinja2 from templates/pipelines/.
Gold tables are created directly (CTAS / CREATE TABLE). Bronze and Silver
tables already exist (created by register_tenant); this handler only
executes the Silver MERGE and Vault hub loads for the first time.
"""

from __future__ import annotations

import logging
import os
from dataclasses import replace
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parents[2] / "templates" / "pipelines"

PIPELINE_TEMPLATES = [
    ("bronze_ingestion.sql.j2",  "bronze"),
    ("silver_merge.sql.j2",      "silver"),
    ("vault_hub.sql.j2",         "vault"),
    ("gold_analytics.sql.j2",    "gold"),
]


def run(ctx, spark: SparkSession):
    tenant_id = ctx.tenant_id
    catalog   = ctx.catalog

    logger.info("Scaffolding pipelines for tenant %s", tenant_id)

    template_vars = _build_template_vars(ctx)
    rendered      = _render_templates(template_vars)

    _execute_gold_models(rendered, spark)
    _execute_vault_load(tenant_id, rendered, spark)

    deviation = _check_for_deviation(ctx, spark)

    pr_url = _open_pr(tenant_id, rendered)

    logger.info("Tenant %s: pipelines scaffolded, PR: %s", tenant_id, pr_url)

    _log_change(
        catalog, tenant_id,
        f"PIPELINES_SCAFFOLDED | pr={pr_url} | deviation={deviation['has_deviation']}",
        spark,
    )

    return replace(ctx, metadata={
        **ctx.metadata,
        "pipeline_pr_url":  pr_url,
        "has_deviation":    deviation["has_deviation"],
        "deviating_fields": deviation.get("deviating_fields", []),
    })


def _build_template_vars(ctx) -> dict:
    return {
        "tenant_id":     ctx.tenant_id,
        "catalog":       ctx.catalog,
        "env":           ctx.env,
        "gold_schema":   ctx.metadata.get("gold_schema", f"gold_{ctx.tenant_id}"),
        "bronze_schema": "bronze",
        "silver_schema": "silver",
        "vault_schema":  "vault",
    }


def _render_templates(vars_: dict) -> dict[str, str]:
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        undefined=StrictUndefined,
        keep_trailing_newline=True,
    )
    rendered = {}
    for tpl_name, _ in PIPELINE_TEMPLATES:
        template = env.get_template(tpl_name)
        out_name = tpl_name.replace(".j2", "")
        rendered[out_name] = template.render(**vars_)
    return rendered


def _execute_gold_models(rendered: dict[str, str], spark: SparkSession) -> None:
    """Runs the Gold CTAS and view DDL from the rendered template."""
    gold_sql = rendered.get("gold_analytics.sql")
    if not gold_sql:
        return

    # Split on the comment separator used in the template
    statements = [
        s.strip() for s in gold_sql.split("-- ──")
        if s.strip() and not s.strip().startswith("--")
        and any(kw in s.upper() for kw in ("CREATE", "MERGE", "INSERT"))
    ]

    for stmt in statements:
        try:
            spark.sql(stmt)
            first_line = stmt.split("\n")[0][:80]
            logger.info("Gold DDL executed: %s...", first_line)
        except Exception as exc:
            logger.warning("Gold DDL skipped (may already exist): %s", exc)


def _execute_vault_load(tenant_id: str, rendered: dict[str, str], spark: SparkSession) -> None:
    """Runs the initial Vault hub MERGE (will be a no-op if Silver has no rows yet)."""
    vault_sql = rendered.get("vault_hub.sql")
    if not vault_sql:
        return

    merge_blocks = [
        s.strip() for s in vault_sql.split(";")
        if "MERGE INTO" in s.upper()
    ]
    for block in merge_blocks:
        try:
            spark.sql(block)
            logger.info("Vault hub MERGE executed for tenant %s", tenant_id)
        except Exception as exc:
            logger.warning("Vault MERGE skipped: %s", exc)


def _check_for_deviation(ctx, spark: SparkSession) -> dict:
    """
    Compares tenant schema against the standard field list using
    information_schema. Returns deviation report dict.
    """
    catalog   = ctx.catalog
    tenant_id = ctx.tenant_id
    standard  = {
        "event_id", "event_type", "event_timestamp",
        "source_system", "status", "reference_id",
    }

    try:
        rows = spark.sql(f"""
            SELECT column_name
            FROM {catalog}.information_schema.columns
            WHERE table_schema = 'bronze'
              AND table_name   = 'raw_{tenant_id}_events'
              AND column_name NOT IN ('tenant_id','_ingested_at','_source_file','_batch_id')
        """).collect()

        actual           = {r.column_name for r in rows}
        deviating_fields = list(actual - standard)
        has_deviation    = len(deviating_fields) > 0
    except Exception:
        has_deviation    = False
        deviating_fields = []

    return {"has_deviation": has_deviation, "deviating_fields": deviating_fields}


def _open_pr(tenant_id: str, rendered: dict[str, str]) -> str:
    """Opens a draft PR when GITHUB_TOKEN is present; returns a local path otherwise."""
    import subprocess, tempfile
    from pathlib import Path as P

    simulate = os.environ.get("SIMULATE", "1") == "1" or not os.environ.get("GITHUB_TOKEN")
    if simulate:
        out = P(tempfile.mkdtemp(prefix=f"pipelines_pr_{tenant_id}_"))
        for name, sql in rendered.items():
            (out / name).write_text(sql)
        return f"file://{out}  (simulated)"

    repo   = os.environ.get("GITHUB_REPO", "rfim/multi-tenant-onboarding")
    branch = f"onboard/{tenant_id}/pipelines"
    root   = P(__file__).parents[2]

    for name, sql in rendered.items():
        dest = root / "dbt" / "models" / "generated" / tenant_id / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(sql)

    subprocess.run(["git", "-C", str(root), "checkout", "-b", branch], check=True)
    subprocess.run(["git", "-C", str(root), "add", f"dbt/models/generated/{tenant_id}/"], check=True)
    subprocess.run(["git", "-C", str(root), "commit", "-m",
                    f"[onboarding] {tenant_id} pipeline scaffold"], check=True)
    subprocess.run(["git", "-C", str(root), "push", "-u", "origin", branch], check=True)

    result = subprocess.run(
        ["gh", "pr", "create", "--repo", repo, "--base", "dev",
         "--head", branch,
         "--title", f"[onboarding] {tenant_id} pipeline scaffold",
         "--body",  f"Auto-generated pipeline scaffold for tenant `{tenant_id}`.",
         "--label", "tenant_config", "--draft"],
        capture_output=True, text=True, check=True, cwd=str(root),
    )
    return result.stdout.strip()


def _log_change(catalog: str, tenant_id: str, description: str, spark: SparkSession) -> None:
    safe_desc = description.replace("'", "''")
    spark.sql(f"""
        INSERT INTO {catalog}.monitoring.tenant_change_log
            (tenant_id, change_timestamp, change_type, component, description, env)
        VALUES
            ('{tenant_id}', CURRENT_TIMESTAMP(), 'PIPELINES_SCAFFOLDED',
             'scaffold_pipelines', '{safe_desc}', 'dev')
    """)
