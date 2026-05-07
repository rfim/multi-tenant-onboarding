"""
Handler: PIPELINES_SCAFFOLDED -> DASHBOARDS_SCAFFOLDED

Clones certified Superset dashboard templates and parameterises them with
the new tenant ID. Adds the dashboards to the customer's workspace.

Template selection:
  By default, all five standard templates are applied:
    - kpi_overview
    - approval_volume
    - cycle_time
    - vendor_analytics
    - user_activity

  If the onboarding payload includes a customer_profile dict, an optional
  AI suggestion step can narrow the list. The suggestion is advisory only;
  the full set is applied unless the engineer explicitly removes templates
  from the payload.

Superset integration:
  Uses the Superset REST API (or Preset API if hosted). Credentials are
  read from Databricks Secrets, never from environment variables or code.
"""

from __future__ import annotations

import logging
from dataclasses import replace

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

DEFAULT_DASHBOARD_TEMPLATES = [
    "kpi_overview",
    "approval_volume",
    "cycle_time",
    "vendor_analytics",
    "user_activity",
]


def run(ctx, spark: SparkSession):
    """
    Clones dashboard templates and adds them to the tenant's workspace.

    Args:
        ctx: TenantContext with state == PIPELINES_SCAFFOLDED
        spark: active SparkSession

    Returns:
        Updated TenantContext with dashboard_ids in metadata.
    """
    tenant_id = ctx.tenant_id
    templates_to_apply = ctx.metadata.get("dashboard_templates", DEFAULT_DASHBOARD_TEMPLATES)

    logger.info(
        "Scaffolding dashboards for tenant %s: %s",
        tenant_id, templates_to_apply,
    )

    dashboard_ids = {}
    for template_name in templates_to_apply:
        dashboard_id = _clone_and_parameterise(tenant_id, template_name, ctx.env)
        dashboard_ids[template_name] = dashboard_id
        logger.debug("Cloned %s -> dashboard_id=%s", template_name, dashboard_id)

    # Register dashboard IDs in monitoring so the changelog summariser can link to them
    _register_dashboards(ctx.catalog, tenant_id, dashboard_ids, spark)

    logger.info("Tenant %s: %d dashboards provisioned.", tenant_id, len(dashboard_ids))

    return replace(ctx, metadata={**ctx.metadata, "dashboard_ids": dashboard_ids})


def _clone_and_parameterise(tenant_id: str, template_name: str, env: str) -> str:
    """
    Clones a Superset dashboard template and substitutes tenant-specific values.

    Returns the new dashboard's ID.
    """
    # TODO:
    # 1. Load template from templates/dashboards/<template_name>.json.j2
    # 2. Render with Jinja2 using tenant_id and catalog name
    # 3. POST to Superset /api/v1/dashboard/import with rendered JSON
    # 4. Return the created dashboard ID
    raise NotImplementedError(f"_clone_and_parameterise not implemented for {template_name}")


def _register_dashboards(catalog: str, tenant_id: str, dashboard_ids: dict, spark: SparkSession) -> None:
    """Writes dashboard metadata to monitoring.tenant_dashboards."""
    # TODO: spark.createDataFrame([...]).write.format("delta").mode("append")
    #       .saveAsTable(f"{catalog}.monitoring.tenant_dashboards")
    pass
