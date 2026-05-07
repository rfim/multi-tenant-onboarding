"""
Handler: GRANTS_PROVISIONED -> PIPELINES_SCAFFOLDED

Renders Bronze/Silver/Vault/Gold SQL templates for the new tenant and opens
a pull request against the dev branch. The PR is opened by the platform
service account. A human engineer reviews and merges the PR; this handler
does not merge it.

If the tenant's schema deviates from the standard templates, this handler
invokes the deviation_detector agent, which opens a separate PR with adapted
models. The deviation is recorded in ctx.metadata so subsequent handlers know
the PR reference.

Template rendering uses Jinja2. Templates live in templates/pipelines/.
"""

from __future__ import annotations

import logging
import os
import tempfile
from dataclasses import replace
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parents[2] / "templates" / "pipelines"

PIPELINE_TEMPLATES = [
    "bronze_ingestion.sql.j2",
    "silver_merge.sql.j2",
    "vault_hub.sql.j2",
    "gold_analytics.sql.j2",
]


def run(ctx, spark: SparkSession):
    """
    Renders pipeline templates and opens a draft PR.

    Args:
        ctx: TenantContext with state == GRANTS_PROVISIONED
        spark: active SparkSession

    Returns:
        Updated TenantContext with pr_url in metadata.
    """
    tenant_id = ctx.tenant_id
    catalog = ctx.catalog

    logger.info("Scaffolding pipelines for tenant %s", tenant_id)

    template_vars = _build_template_vars(ctx)
    rendered = _render_templates(template_vars)

    # Check for schema deviation before opening the standard PR
    deviation_result = _check_for_deviation(ctx, spark, template_vars)

    if deviation_result["has_deviation"]:
        logger.warning(
            "Tenant %s has schema deviations. Invoking deviation detector.",
            tenant_id,
        )
        # Deviation detector runs asynchronously via a Databricks Workflow trigger.
        # This handler records the deviation and continues; the main PR is still
        # opened for the standard templates. The deviation detector opens a
        # separate PR for the adapted models.
        # TODO: trigger agents.deviation_detector workflow via Databricks SDK
        ctx = replace(ctx, metadata={**ctx.metadata, "has_deviation": True})

    pr_url = _open_pr(tenant_id, rendered, dry_run=ctx.env == "dev")

    logger.info("Tenant %s: pipeline PR opened at %s", tenant_id, pr_url)

    return replace(ctx, metadata={**ctx.metadata, "pipeline_pr_url": pr_url})


def _build_template_vars(ctx) -> dict:
    """Builds the variable dict passed to every Jinja2 template."""
    return {
        "tenant_id": ctx.tenant_id,
        "catalog": ctx.catalog,
        "env": ctx.env,
        "gold_schema": ctx.metadata.get("gold_schema", f"gold_{ctx.tenant_id}"),
        "bronze_schema": "bronze",
        "silver_schema": "silver",
        "vault_schema": "vault",
        # TODO: extend with customer-specific parameters from the onboarding payload
        # e.g. source_system, primary_key_columns, date_column
    }


def _render_templates(vars_: dict) -> dict[str, str]:
    """Renders all pipeline templates and returns filename -> SQL content."""
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        undefined=StrictUndefined,
        keep_trailing_newline=True,
    )
    rendered = {}
    for tpl_name in PIPELINE_TEMPLATES:
        template = env.get_template(tpl_name)
        output_name = tpl_name.replace(".j2", "").replace(
            "ingestion", f"{vars_['tenant_id']}_ingestion"
        ).replace("merge", f"{vars_['tenant_id']}_merge")
        rendered[output_name] = template.render(**vars_)
    return rendered


def _check_for_deviation(ctx, spark: SparkSession, template_vars: dict) -> dict:
    """
    Compares the tenant's actual source schema against template expectations.

    Returns a dict with:
      has_deviation: bool
      deviating_fields: list[str]
    """
    # TODO: query information_schema or the source system's metadata API
    # to retrieve the actual schema, then compare against the field list
    # encoded in the template variables.
    return {"has_deviation": False, "deviating_fields": []}


def _open_pr(tenant_id: str, rendered: dict[str, str], dry_run: bool) -> str:
    """
    Creates a branch, writes rendered SQL files, and opens a GitHub PR.

    Returns the PR URL.
    """
    if dry_run:
        logger.info("[dry-run] Would open PR for tenant %s", tenant_id)
        return "https://github.com/example/multi-tenant-onboarding/pull/0"

    # TODO: implement via PyGithub or GitHub REST API
    # 1. Create branch: onboard/<tenant_id>/pipelines
    # 2. Write rendered SQL files to dbt/models/<schema>/<tenant_id>/
    # 3. Open PR against dev with title "[onboarding] <tenant_id> pipeline scaffold"
    # 4. Add label: tenant_config
    # 5. Return PR URL
    raise NotImplementedError("_open_pr not yet implemented")
