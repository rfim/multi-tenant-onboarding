"""
Handler: QUALITY_SCAFFOLDED -> ACTIVE

Final step of the onboarding state machine. Marks the tenant as ACTIVE in
monitoring.tenant_registry and sends the onboarding completion notification.

Pre-activation checks:
  1. Gold schema exists and is accessible by the tenant SP
  2. At least one DQ contract is registered for the tenant
  3. The pipeline scaffold PR exists (may not yet be merged; that is acceptable)
  4. No open FAILED runs in monitoring.agent_errors for this tenant

If any check fails, the handler raises and the machine transitions to FAILED.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime, timezone

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def run(ctx, spark: SparkSession):
    """
    Validates pre-activation checks and marks the tenant ACTIVE.

    Args:
        ctx: TenantContext with state == QUALITY_SCAFFOLDED
        spark: active SparkSession

    Returns:
        Updated TenantContext.

    Raises:
        RuntimeError if any pre-activation check fails.
    """
    tenant_id = ctx.tenant_id
    catalog = ctx.catalog

    logger.info("Running pre-activation checks for tenant %s", tenant_id)

    _check_gold_schema(catalog, tenant_id, spark)
    _check_dq_contracts(catalog, tenant_id, spark)
    _check_no_agent_errors(catalog, tenant_id, spark)

    _mark_active(catalog, tenant_id, spark)
    _send_notification(ctx)

    logger.info("Tenant %s is now ACTIVE.", tenant_id)

    return replace(
        ctx,
        metadata={
            **ctx.metadata,
            "activated_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def _check_gold_schema(catalog: str, tenant_id: str, spark: SparkSession) -> None:
    """Verifies the Gold schema was created by register_tenant."""
    # TODO: spark.sql(f"DESCRIBE SCHEMA {catalog}.gold_{tenant_id}")
    # Raises AnalysisException if schema does not exist -> caught by caller
    pass


def _check_dq_contracts(catalog: str, tenant_id: str, spark: SparkSession) -> None:
    """Verifies at least one DQ contract is registered."""
    # TODO: query monitoring.dq_contracts where tenant_id = ... and is_active = true
    # Raise RuntimeError if count == 0
    pass


def _check_no_agent_errors(catalog: str, tenant_id: str, spark: SparkSession) -> None:
    """Verifies no unresolved agent errors exist for this tenant."""
    # TODO: query monitoring.agent_errors where tenant_id = ... and resolved = false
    # Raise RuntimeError if any rows found
    pass


def _mark_active(catalog: str, tenant_id: str, spark: SparkSession) -> None:
    """Updates monitoring.tenant_registry to set state = ACTIVE."""
    # TODO: Delta MERGE or UPDATE on monitoring.tenant_registry
    # SET state = 'ACTIVE', activated_at = current_timestamp()
    # WHERE tenant_id = '<tenant_id>'
    pass


def _send_notification(ctx) -> None:
    """Posts an onboarding completion message to Slack and the tenant contact."""
    # TODO: POST to Slack #platform-alerts with tenant_id, activation time, and
    #       links to the provisioned dashboards (from ctx.metadata['dashboard_ids'])
    # Also: trigger the welcome email flow via the CRM webhook if configured
    pass
