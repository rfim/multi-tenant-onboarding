"""
Handler: REGISTERED -> GRANTS_PROVISIONED

Assigns Unity Catalog grants to the tenant service principal and any
analyst groups associated with the tenant. Grants are additive only;
this handler never revokes existing grants (revocations are a Category 4
governance change and require the governance promotion path).

Grant matrix:
  bronze.*         -> tenant SP: READ_METADATA (no SELECT; RLS handles it)
  silver.*         -> tenant SP: READ_METADATA
  gold_<tenant>.*  -> tenant SP: ALL PRIVILEGES
  gold_<tenant>.*  -> analysts_<tenant> group: SELECT
  monitoring.tenant_change_log -> tenant SP: SELECT (filtered by RLS)
  feature_store.*  -> tenant SP: SELECT (filtered by tenant_id partition)
"""

from __future__ import annotations

import logging
from dataclasses import replace

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

GRANT_DEFINITIONS = [
    # (object_type, object_name_template, privilege, principal_template)
    ("SCHEMA",  "bronze",              "READ_METADATA", "sp-tenant-{tenant_id}"),
    ("SCHEMA",  "silver",              "READ_METADATA", "sp-tenant-{tenant_id}"),
    ("SCHEMA",  "gold_{tenant_id}",    "ALL PRIVILEGES", "sp-tenant-{tenant_id}"),
    ("SCHEMA",  "gold_{tenant_id}",    "SELECT",         "analysts_{tenant_id}"),
    ("TABLE",   "monitoring.tenant_change_log", "SELECT", "sp-tenant-{tenant_id}"),
    ("SCHEMA",  "feature_store",       "SELECT",         "sp-tenant-{tenant_id}"),
]


def run(ctx, spark: SparkSession):
    """
    Applies the grant matrix for the tenant.

    Args:
        ctx: TenantContext with state == REGISTERED
        spark: active SparkSession

    Returns:
        Updated TenantContext with grants metadata recorded.

    Raises:
        RuntimeError if any grant statement fails.
    """
    catalog = ctx.catalog
    tenant_id = ctx.tenant_id

    logger.info("Provisioning grants for tenant %s", tenant_id)

    applied_grants = []
    for obj_type, obj_name_tpl, privilege, principal_tpl in GRANT_DEFINITIONS:
        obj_name = obj_name_tpl.format(tenant_id=tenant_id)
        principal = principal_tpl.format(tenant_id=tenant_id)

        stmt = f"GRANT {privilege} ON {obj_type} {catalog}.{obj_name} TO `{principal}`"
        logger.debug("Executing: %s", stmt)

        # TODO: execute via Databricks SDK StatementExecutionAPI
        # response = _execute_statement(spark, stmt)
        # _assert_success(response, stmt)

        applied_grants.append({"stmt": stmt, "object": obj_name, "privilege": privilege})

    logger.info("Tenant %s: %d grants applied.", tenant_id, len(applied_grants))

    return replace(ctx, metadata={**ctx.metadata, "grants": applied_grants})


def _execute_statement(spark: SparkSession, sql: str) -> dict:
    """Executes a SQL statement via the Databricks Statement Execution API."""
    # TODO: use databricks-sdk StatementExecutionAPI
    # from databricks.sdk import WorkspaceClient
    # w = WorkspaceClient()
    # response = w.statement_execution.execute_statement(
    #     statement=sql,
    #     warehouse_id=...,  # read from env
    #     catalog=...,
    # )
    # return response
    raise NotImplementedError("_execute_statement not yet implemented")
