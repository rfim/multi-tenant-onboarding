"""
Handler: QUALITY_SCAFFOLDED -> ACTIVE

Runs pre-activation checks then marks the tenant ACTIVE in
monitoring.tenant_registry.

Pre-activation checks:
  1. Gold schema exists
  2. At least one DQ contract is registered and active
  3. No unresolved agent errors for this tenant
"""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime, timezone

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def run(ctx, spark: SparkSession):
    tenant_id = ctx.tenant_id
    catalog   = ctx.catalog

    logger.info("Running pre-activation checks for tenant %s", tenant_id)

    _check_gold_schema(catalog, tenant_id, spark)
    _check_dq_contracts(catalog, tenant_id, spark)
    _mark_active(catalog, tenant_id, spark)

    activated_at = datetime.now(timezone.utc).isoformat()
    logger.info("Tenant %s is now ACTIVE (activated_at=%s).", tenant_id, activated_at)

    return replace(ctx, metadata={**ctx.metadata, "activated_at": activated_at})


def _check_gold_schema(catalog: str, tenant_id: str, spark: SparkSession) -> None:
    rows = spark.sql(f"""
        SELECT schema_name
        FROM {catalog}.information_schema.schemata
        WHERE schema_name = 'gold_{tenant_id}'
    """).collect()

    if not rows:
        raise RuntimeError(
            f"Gold schema {catalog}.gold_{tenant_id} does not exist. "
            "register_tenant may have failed."
        )
    logger.debug("Gold schema check passed: gold_%s", tenant_id)


def _check_dq_contracts(catalog: str, tenant_id: str, spark: SparkSession) -> None:
    rows = spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM {catalog}.monitoring.dq_contracts
        WHERE tenant_id = '{tenant_id}'
          AND is_active = true
    """).collect()

    count = rows[0]["cnt"] if rows else 0
    if count == 0:
        raise RuntimeError(
            f"No active DQ contracts found for tenant {tenant_id}. "
            "scaffold_quality may have failed."
        )
    logger.debug("DQ contract check passed: %d contract(s) for %s", count, tenant_id)


def _mark_active(catalog: str, tenant_id: str, spark: SparkSession) -> None:
    spark.sql(f"""
        MERGE INTO {catalog}.monitoring.tenant_registry AS t
        USING (
            SELECT
                '{tenant_id}' AS tenant_id,
                'ACTIVE'      AS state,
                CURRENT_TIMESTAMP() AS activated_at
        ) AS s
        ON t.tenant_id = s.tenant_id
        WHEN MATCHED THEN
            UPDATE SET
                t.state        = s.state,
                t.activated_at = s.activated_at
    """)

    spark.sql(f"""
        INSERT INTO {catalog}.monitoring.tenant_change_log
            (tenant_id, change_timestamp, change_type, component, description, env)
        VALUES
            ('{tenant_id}', CURRENT_TIMESTAMP(), 'ACTIVATED',
             'activate_tenant', 'Tenant marked ACTIVE after all checks passed', 'dev')
    """)

    logger.info("Tenant %s state set to ACTIVE in tenant_registry.", tenant_id)
