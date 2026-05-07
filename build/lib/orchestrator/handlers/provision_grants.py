"""
Handler: REGISTERED -> GRANTS_PROVISIONED

Assigns Unity Catalog grants to the tenant service principal and analyst group.
Grants are additive only; this handler never revokes existing grants.

Grant matrix
------------
bronze.*                   -> tenant SP: READ_METADATA
silver.*                   -> tenant SP: READ_METADATA
gold_<tenant>.*            -> tenant SP: ALL PRIVILEGES
gold_<tenant>.*            -> analysts_<tenant> group: SELECT
feature_store.*            -> tenant SP: SELECT
monitoring.tenant_change_log -> tenant SP: SELECT
"""

from __future__ import annotations

import logging
from dataclasses import replace

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# (privilege, object_type, object_name_template, principal_template)
GRANT_MATRIX = [
    ("READ_METADATA",  "SCHEMA", "bronze",                  "sp-tenant-{t}"),
    ("READ_METADATA",  "SCHEMA", "silver",                  "sp-tenant-{t}"),
    ("ALL PRIVILEGES", "SCHEMA", "gold_{t}",                "sp-tenant-{t}"),
    ("SELECT",         "SCHEMA", "gold_{t}",                "analysts_{t}"),
    ("SELECT",         "SCHEMA", "feature_store",           "sp-tenant-{t}"),
    ("SELECT",         "TABLE",  "monitoring.tenant_change_log", "sp-tenant-{t}"),
]


def run(ctx, spark: SparkSession):
    catalog   = ctx.catalog
    tenant_id = ctx.tenant_id

    logger.info("Provisioning grants for tenant %s", tenant_id)

    applied: list[str] = []
    skipped: list[str] = []

    for privilege, obj_type, obj_tpl, principal_tpl in GRANT_MATRIX:
        obj_name  = obj_tpl.format(t=tenant_id)
        principal = principal_tpl.format(t=tenant_id)

        try:
            spark.sql(
                f"GRANT {privilege} ON {obj_type} {catalog}.{obj_name} TO `{principal}`"
            )
            applied.append(f"{privilege} ON {obj_name} -> {principal}")
        except Exception as exc:
            # Unity Catalog raises if the principal does not exist yet (e.g. the
            # service principal hasn't been created in the IdP). Log and continue;
            # the missing grant will surface in the activate_tenant pre-check.
            logger.warning(
                "Grant skipped (principal may not exist yet): %s ON %s -> %s | %s",
                privilege, obj_name, principal, exc,
            )
            skipped.append(f"{privilege} ON {obj_name} -> {principal}")

    _log_change(catalog, tenant_id, "GRANTS", f"{len(applied)} applied, {len(skipped)} skipped", spark)
    logger.info("Tenant %s: %d grants applied, %d skipped.", tenant_id, len(applied), len(skipped))

    return replace(ctx, metadata={
        **ctx.metadata,
        "grants_applied": applied,
        "grants_skipped": skipped,
    })


def _log_change(catalog: str, tenant_id: str, change_type: str, description: str, spark: SparkSession) -> None:
    spark.sql(f"""
        INSERT INTO {catalog}.monitoring.tenant_change_log
            (tenant_id, change_timestamp, change_type, component, description, env)
        VALUES
            ('{tenant_id}', CURRENT_TIMESTAMP(), '{change_type}',
             'provision_grants', '{description}', 'dev')
    """)
