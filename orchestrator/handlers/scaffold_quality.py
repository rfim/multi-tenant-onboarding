"""
Handler: DASHBOARDS_SCAFFOLDED -> QUALITY_SCAFFOLDED

Renders DQ contract templates and registers them in monitoring.dq_contracts.
The rendered YAML files are written to the repo working tree so the next
git commit (via the pipeline PR) includes them.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

CONTRACTS_DIR = Path(__file__).parents[2] / "templates" / "quality_contracts"

CONTRACT_TEMPLATES = [
    ("bronze_contract.yml.j2", "bronze"),
    ("silver_contract.yml.j2", "silver"),
    ("gold_contract.yml.j2",   "gold"),
]


def run(ctx, spark: SparkSession):
    tenant_id = ctx.tenant_id
    catalog   = ctx.catalog

    logger.info("Scaffolding DQ contracts for tenant %s", tenant_id)

    rendered = _render_contracts(ctx)
    _write_contracts(rendered)
    _register_contracts(catalog, tenant_id, rendered, spark)
    _log_change(catalog, tenant_id, f"{len(rendered)} DQ contracts registered", spark)

    logger.info("Tenant %s: %d DQ contracts instantiated.", tenant_id, len(rendered))

    return replace(ctx, metadata={
        **ctx.metadata,
        "dq_contract_paths": list(rendered.keys()),
    })


def _render_contracts(ctx) -> dict[str, str]:
    from datetime import datetime, timezone, timedelta

    now = datetime.now(timezone.utc)
    env = Environment(
        loader=FileSystemLoader(str(CONTRACTS_DIR)),
        undefined=StrictUndefined,
        keep_trailing_newline=True,
    )
    vars_ = {
        "tenant_id":         ctx.tenant_id,
        "catalog":           ctx.catalog,
        "gold_schema":       ctx.metadata.get("gold_schema", f"gold_{ctx.tenant_id}"),
        "now_utc":           now.isoformat(),
        "lookback_start":    (now - timedelta(days=90)).isoformat(),
        "tomorrow":          (now + timedelta(days=1)).isoformat(),
        "yesterday_minus_2h": (now - timedelta(hours=26)).isoformat(),
        "today":             now.date().isoformat(),
    }
    rendered = {}
    for tpl_name, layer in CONTRACT_TEMPLATES:
        output_path = f"dq/expectations/{ctx.tenant_id}/{layer}_contract.yml"
        rendered[output_path] = env.get_template(tpl_name).render(**vars_)
    return rendered


def _write_contracts(contracts: dict[str, str]) -> None:
    """Writes rendered contract files to the repo working tree."""
    repo_root = Path(__file__).parents[2]
    for rel_path, content in contracts.items():
        dest = repo_root / rel_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(content)
        logger.info("Written: %s", rel_path)


def _register_contracts(
    catalog: str, tenant_id: str, contracts: dict[str, str], spark: SparkSession
) -> None:
    for contract_path, _ in contracts.items():
        layer = Path(contract_path).stem.replace("_contract", "")
        spark.sql(f"""
            MERGE INTO {catalog}.monitoring.dq_contracts AS t
            USING (
                SELECT
                    '{tenant_id}'    AS tenant_id,
                    '{layer}'        AS layer,
                    '{contract_path}' AS contract_path,
                    CURRENT_TIMESTAMP() AS created_at,
                    true             AS is_active
            ) AS s
            ON t.tenant_id = s.tenant_id AND t.layer = s.layer
            WHEN NOT MATCHED THEN
                INSERT (tenant_id, layer, contract_path, created_at, is_active)
                VALUES (s.tenant_id, s.layer, s.contract_path, s.created_at, s.is_active)
        """)
    logger.debug("Registered %d contracts for tenant %s", len(contracts), tenant_id)


def _log_change(catalog: str, tenant_id: str, description: str, spark: SparkSession) -> None:
    spark.sql(f"""
        INSERT INTO {catalog}.monitoring.tenant_change_log
            (tenant_id, change_timestamp, change_type, component, description, env)
        VALUES
            ('{tenant_id}', CURRENT_TIMESTAMP(), 'QUALITY_SCAFFOLDED',
             'scaffold_quality', '{description}', 'dev')
    """)
