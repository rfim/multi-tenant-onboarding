"""
Handler: DASHBOARDS_SCAFFOLDED -> QUALITY_SCAFFOLDED

Instantiates per-tenant data quality contracts from the baseline templates.
Baseline contracts apply identically to every tenant; this handler renders
them with the tenant's catalog and schema parameters.

DQ stack:
  - Great Expectations: column-level expectations (null rates, value ranges,
    format patterns). Suite per layer: bronze, silver, gold.
  - dbt tests: referential integrity and business-rule assertions.

Output:
  - GE expectation suite files written to dq/expectations/<tenant_id>/
  - dbt schema.yml files written to dbt/models/<schema>/<tenant_id>/
  - Both are committed to a branch and included in the pipeline scaffold PR
    (or a separate PR if the pipeline PR has already been merged).

Per-tenant DQ results are written to monitoring.dq_results, partitioned by
tenant_id and run_date.
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
    ("bronze_contract.yml.j2",  "bronze"),
    ("silver_contract.yml.j2",  "silver"),
    ("gold_contract.yml.j2",    "gold"),
]


def run(ctx, spark: SparkSession):
    """
    Renders DQ contracts and registers them in the monitoring schema.

    Args:
        ctx: TenantContext with state == DASHBOARDS_SCAFFOLDED
        spark: active SparkSession

    Returns:
        Updated TenantContext with dq_contract_paths in metadata.
    """
    tenant_id = ctx.tenant_id
    catalog = ctx.catalog

    logger.info("Scaffolding DQ contracts for tenant %s", tenant_id)

    rendered_contracts = _render_contracts(ctx)
    _write_contracts_to_repo(tenant_id, rendered_contracts)
    _register_contracts(catalog, tenant_id, rendered_contracts, spark)

    logger.info("Tenant %s: %d DQ contracts instantiated.", tenant_id, len(rendered_contracts))

    return replace(
        ctx,
        metadata={**ctx.metadata, "dq_contract_paths": list(rendered_contracts.keys())},
    )


def _render_contracts(ctx) -> dict[str, str]:
    """Renders each contract template with tenant parameters."""
    env = Environment(
        loader=FileSystemLoader(str(CONTRACTS_DIR)),
        undefined=StrictUndefined,
        keep_trailing_newline=True,
    )
    vars_ = {
        "tenant_id": ctx.tenant_id,
        "catalog": ctx.catalog,
        "gold_schema": ctx.metadata.get("gold_schema", f"gold_{ctx.tenant_id}"),
    }
    rendered = {}
    for tpl_name, layer in CONTRACT_TEMPLATES:
        template = env.get_template(tpl_name)
        output_path = f"dq/expectations/{ctx.tenant_id}/{layer}_contract.yml"
        rendered[output_path] = template.render(**vars_)
    return rendered


def _write_contracts_to_repo(tenant_id: str, contracts: dict[str, str]) -> None:
    """
    Writes rendered contract files to the repository branch opened by
    scaffold_pipelines. If that PR has already been merged, opens a new branch.
    """
    # TODO: write files to git branch onboard/<tenant_id>/pipelines or
    #       onboard/<tenant_id>/quality if the pipelines branch is gone
    pass


def _register_contracts(
    catalog: str,
    tenant_id: str,
    contracts: dict[str, str],
    spark: SparkSession,
) -> None:
    """Appends contract metadata to monitoring.dq_contracts."""
    # TODO: write rows to monitoring.dq_contracts with:
    #   tenant_id, layer, contract_path, created_at, is_active=true
    pass
