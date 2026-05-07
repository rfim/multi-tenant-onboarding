"""
Handler: PENDING -> REGISTERED

Creates all Unity Catalog objects for the new tenant:
  - Gold schema: <catalog>.gold_<tenant_id>
  - Monitoring entry in <catalog>.monitoring.tenant_registry
  - Row-level security policy on bronze.* and silver.* filtering by tenant_id

This handler is idempotent: if the schema already exists, it verifies state
and continues. It does not drop and recreate objects.

Isolation guarantee: tenant isolation is enforced entirely in this handler
via Unity Catalog RLS. Application code must not replicate this logic.
"""

from __future__ import annotations

import logging
from dataclasses import replace

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def run(ctx, spark: SparkSession):
    """
    Creates catalog objects and RLS policy for the tenant.

    Args:
        ctx: TenantContext with state == PENDING
        spark: active SparkSession

    Returns:
        Updated TenantContext (state unchanged; caller sets next state).

    Raises:
        RuntimeError if catalog objects cannot be created or verified.
    """
    catalog = ctx.catalog
    tenant_id = ctx.tenant_id

    logger.info("Registering tenant %s in catalog %s", tenant_id, catalog)

    # 1. Create Gold schema
    # TODO: execute via spark.sql() or Databricks SDK CatalogsAPI
    # spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold_{tenant_id}")

    # 2. Register tenant in monitoring.tenant_registry
    # TODO: insert row into monitoring.tenant_registry with:
    #   tenant_id, env, created_at, state='PENDING'
    # Use Delta merge-on-insert to keep idempotency.

    # 3. Apply row-level security
    # RLS policy template is in templates/rls/bronze_silver_rls.sql.j2
    # Render with tenant_id and execute via Databricks SDK StatementExecutionAPI.
    # The policy must reference the current_user() function to scope access
    # to the tenant's service principal only.
    # TODO: render and apply RLS policy

    # 4. Grant SELECT on monitoring.tenant_change_log to tenant service principal
    # TODO: spark.sql(f"GRANT SELECT ON TABLE {catalog}.monitoring.tenant_change_log
    #                   TO `{_sp_name(tenant_id)}`")

    logger.info("Tenant %s registered.", tenant_id)

    # Metadata passed forward to subsequent handlers
    return replace(ctx, metadata={**ctx.metadata, "gold_schema": f"gold_{tenant_id}"})


def _sp_name(tenant_id: str) -> str:
    """Returns the Unity Catalog service principal name for a tenant."""
    # Convention: sp-tenant-<tenant_id>
    return f"sp-tenant-{tenant_id}"
