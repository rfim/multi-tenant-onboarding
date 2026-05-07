"""
Handler: PIPELINES_SCAFFOLDED -> DASHBOARDS_SCAFFOLDED

Registers the tenant's dashboard templates in monitoring.tenant_dashboards.
Native Databricks dashboards are rendered inline in the E2E notebook via
displayHTML/display(); this handler records the intent and completes the
state machine transition so the tenant reaches ACTIVE.

A full Superset/Preset integration can be layered on top later by
implementing _publish_to_superset() and calling it from run().
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
    tenant_id = ctx.tenant_id
    catalog   = ctx.catalog
    templates = ctx.metadata.get("dashboard_templates", DEFAULT_DASHBOARD_TEMPLATES)

    logger.info("Scaffolding dashboards for tenant %s: %s", tenant_id, templates)

    _ensure_dashboards_table(catalog, spark)

    dashboard_ids: dict[str, str] = {}
    for template_name in templates:
        dash_id = f"native:{tenant_id}:{template_name}"
        dashboard_ids[template_name] = dash_id
        _register_dashboard(catalog, tenant_id, template_name, dash_id, ctx.env, spark)
        logger.debug("Registered dashboard %s -> %s", template_name, dash_id)

    logger.info("Tenant %s: %d dashboards registered.", tenant_id, len(dashboard_ids))

    return replace(ctx, metadata={**ctx.metadata, "dashboard_ids": dashboard_ids})


def _ensure_dashboards_table(catalog: str, spark: SparkSession) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.monitoring.tenant_dashboards (
            tenant_id       STRING    NOT NULL,
            template_name   STRING    NOT NULL,
            dashboard_id    STRING    NOT NULL,
            env             STRING    NOT NULL,
            registered_at   TIMESTAMP,
            dashboard_type  STRING
        )
        USING DELTA
        PARTITIONED BY (tenant_id)
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)


def _register_dashboard(
    catalog: str,
    tenant_id: str,
    template_name: str,
    dashboard_id: str,
    env: str,
    spark: SparkSession,
) -> None:
    spark.sql(f"""
        MERGE INTO {catalog}.monitoring.tenant_dashboards AS t
        USING (
            SELECT
                '{tenant_id}'     AS tenant_id,
                '{template_name}' AS template_name,
                '{dashboard_id}'  AS dashboard_id,
                '{env}'           AS env,
                CURRENT_TIMESTAMP() AS registered_at,
                'native'          AS dashboard_type
        ) AS s
        ON  t.tenant_id     = s.tenant_id
        AND t.template_name = s.template_name
        WHEN MATCHED THEN
            UPDATE SET dashboard_id = s.dashboard_id, registered_at = s.registered_at
        WHEN NOT MATCHED THEN
            INSERT *
    """)
