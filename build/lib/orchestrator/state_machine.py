"""
Onboarding orchestrator state machine.

Entry point for the Databricks Workflow task. Reads tenant context from the
onboarding queue, advances the state machine one step at a time, and writes
every transition to monitoring.tenant_change_log.

Design constraints:
  - Deterministic: no LLM calls, no probabilistic branching
  - Idempotent: safe to replay from any state
  - Each handler is a pure function: (TenantContext, SparkSession) -> TenantContext
  - Failures are recorded and surfaced to the on-call engineer; the machine
    does not retry automatically
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from orchestrator.states import TenantState
from orchestrator.transitions import assert_legal
from orchestrator.handlers import (
    activate_tenant,
    provision_grants,
    register_tenant,
    scaffold_dashboards,
    scaffold_pipelines,
    scaffold_quality,
)

logger = logging.getLogger(__name__)

# Maps each source state to the handler that advances it
STATE_HANDLERS = {
    TenantState.PENDING:               register_tenant.run,
    TenantState.REGISTERED:            provision_grants.run,
    TenantState.GRANTS_PROVISIONED:    scaffold_pipelines.run,
    TenantState.PIPELINES_SCAFFOLDED:  scaffold_dashboards.run,
    TenantState.DASHBOARDS_SCAFFOLDED: scaffold_quality.run,
    TenantState.QUALITY_SCAFFOLDED:    activate_tenant.run,
}


@dataclass
class TenantContext:
    tenant_id: str
    env: str
    catalog: str
    state: TenantState = TenantState.PENDING
    metadata: dict = field(default_factory=dict)
    error: str | None = None


def advance(ctx: TenantContext, spark: SparkSession, dry_run: bool = False) -> TenantContext:
    """
    Attempts to advance the state machine by one step.

    Calls the handler for the current state, validates the resulting
    transition, logs it, and returns the updated context. On failure,
    transitions to FAILED and records the error without raising.
    """
    if ctx.state.is_terminal:
        logger.info("Tenant %s is already in terminal state %s", ctx.tenant_id, ctx.state)
        return ctx

    handler = STATE_HANDLERS.get(ctx.state)
    if handler is None:
        raise RuntimeError(f"No handler registered for state {ctx.state}")

    target_state = ctx.state.next_state()
    logger.info(
        "Tenant %s: attempting transition %s -> %s",
        ctx.tenant_id, ctx.state.value, target_state.value,
    )

    try:
        if not dry_run:
            ctx = handler(ctx, spark)
        assert_legal(ctx.state, target_state)
        ctx.state = target_state
        _log_transition(ctx, spark, success=True, dry_run=dry_run)

    except Exception as exc:
        logger.error(
            "Tenant %s: handler %s failed: %s",
            ctx.tenant_id, handler.__name__, exc,
            exc_info=True,
        )
        assert_legal(ctx.state, TenantState.FAILED)
        ctx.error = str(exc)
        ctx.state = TenantState.FAILED
        _log_transition(ctx, spark, success=False, dry_run=dry_run)

    return ctx


def run_to_completion(ctx: TenantContext, spark: SparkSession, dry_run: bool = False) -> TenantContext:
    """
    Runs the state machine until it reaches a terminal state.

    Each step calls advance(). If any step results in FAILED, the loop stops.
    """
    while not ctx.state.is_terminal:
        ctx = advance(ctx, spark, dry_run=dry_run)

    if ctx.state == TenantState.ACTIVE:
        logger.info("Tenant %s onboarding complete.", ctx.tenant_id)
    else:
        logger.error(
            "Tenant %s onboarding failed at previous state. Error: %s",
            ctx.tenant_id, ctx.error,
        )

    return ctx


def _log_transition(
    ctx: TenantContext,
    spark: SparkSession,
    success: bool,
    dry_run: bool,
) -> None:
    """Appends a row to monitoring.tenant_change_log."""
    if dry_run:
        logger.debug("[dry-run] Would log transition for tenant %s state %s", ctx.tenant_id, ctx.state)
        return

    # TODO: implement Delta append to monitoring.tenant_change_log
    # Schema: tenant_id, state, success, error, timestamp, env
    # Use spark.createDataFrame([row]).write.format("delta").mode("append")
    #       .saveAsTable(f"{ctx.catalog}.monitoring.tenant_change_log")
    pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the onboarding state machine.")
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--env", required=True, choices=["dev", "staging", "prod"])
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--resume-from-state", default=None, help="Resume from this state (for replay).")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    initial_state = (
        TenantState(args.resume_from_state)
        if args.resume_from_state
        else TenantState.PENDING
    )

    ctx = TenantContext(
        tenant_id=args.tenant_id,
        env=args.env,
        catalog=args.catalog,
        state=initial_state,
    )

    ctx = run_to_completion(ctx, spark, dry_run=args.dry_run)

    if ctx.state == TenantState.FAILED:
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
