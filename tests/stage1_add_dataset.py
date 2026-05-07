"""
Stage 1 test: Add dataset -> create PR.

Simulates a customer signup webhook for tenant 'acme-corp'. The tenant's
source schema contains three non-standard fields (approval_tier, vendor_code,
amount_usd) that trigger the deviation detector.

Steps exercised:
  1. Load tenant payload from fixture
  2. Run the onboarding state machine in dry-run mode (no catalog writes)
  3. Compute schema deviation report
  4. Call the deviation detector LLM (via CODEX_AUTH_JSON) to generate adapted SQL
  5. Open a draft GitHub PR on the dev branch with the adapted models
  6. Print the PR URL and assert it was created

Run:
  CODEX_AUTH_JSON='{"api_key":"..."}' python -m tests.stage1_add_dataset
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

# Allow imports from repo root
sys.path.insert(0, str(Path(__file__).parents[1]))

from agents.deviation_detector.detector import detect, generate_adapted_models, DeviationReport
from agents.deviation_detector.pr_writer import open_pr, _build_pr_body
from orchestrator.state_machine import TenantContext, run_to_completion
from orchestrator.states import TenantState

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("stage1")

FIXTURE = Path(__file__).parent / "fixtures" / "sample_tenant_payload.json"


def main() -> None:
    payload = json.loads(FIXTURE.read_text())
    tenant_id = payload["tenant_id"]
    catalog    = payload["catalog"]
    env        = payload["env"]
    source_schema = payload["source_schema"]

    logger.info("=" * 60)
    logger.info("STAGE 1: Add dataset -> Create PR")
    logger.info("Tenant: %s | Env: %s | Catalog: %s", tenant_id, env, catalog)
    logger.info("=" * 60)

    # ── Step 1: Dry-run state machine ────────────────────────────────────────
    logger.info("[1/4] Running onboarding state machine (dry-run)...")

    ctx = TenantContext(
        tenant_id=tenant_id,
        env=env,
        catalog=catalog,
        metadata={"dashboard_templates": payload.get("dashboard_templates", [])},
    )

    # Use a lightweight mock spark session for dry-run (no real cluster needed)
    spark = _mock_spark()
    ctx = run_to_completion(ctx, spark, dry_run=True)

    if ctx.state == TenantState.FAILED:
        logger.error("State machine failed during dry-run: %s", ctx.error)
        sys.exit(1)

    logger.info("[1/4] State machine dry-run passed. Final state: %s", ctx.state)

    # ── Step 2: Detect schema deviations ────────────────────────────────────
    logger.info("[2/4] Detecting schema deviations...")

    report = detect(tenant_id=tenant_id, actual_schema=source_schema)

    logger.info(
        "[2/4] Deviation report: %d extra fields, %d missing fields, %d type mismatches",
        len(report.extra_fields),
        len(report.missing_fields),
        len(report.type_mismatches),
    )
    logger.info("  Extra fields  : %s", report.extra_fields)
    logger.info("  Missing fields: %s", report.missing_fields)
    logger.info("  Type mismatches: %s", report.type_mismatches)

    assert report.extra_fields, "Expected non-standard fields (approval_tier, vendor_code, amount_usd)"

    # ── Step 3: Generate adapted SQL via Codex ───────────────────────────────
    logger.info("[3/4] Calling Codex to generate adapted Silver models...")

    adapted_models = generate_adapted_models(report=report, catalog=catalog)

    if not adapted_models:
        logger.error("[3/4] LLM returned no SQL files. Check CODEX_AUTH_JSON and model availability.")
        sys.exit(1)

    logger.info("[3/4] Codex generated %d adapted model(s):", len(adapted_models))
    for fname, sql in adapted_models.items():
        line_count = sql.count("\n")
        logger.info("  %s (%d lines)", fname, line_count)

    # ── Step 4: Open draft PR ────────────────────────────────────────────────
    logger.info("[4/4] Opening draft PR on dev branch...")

    deviation_summary = _format_deviation_summary(report, payload.get("customer_profile", {}))

    pr_url = open_pr(
        tenant_id=tenant_id,
        models=adapted_models,
        deviation_summary=deviation_summary,
    )

    logger.info("[4/4] PR created: %s", pr_url)

    # ── Summary ──────────────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    logger.info("STAGE 1 COMPLETE")
    logger.info("  Tenant ID     : %s", tenant_id)
    logger.info("  Extra fields  : %s", report.extra_fields)
    logger.info("  Models in PR  : %s", list(adapted_models.keys()))
    logger.info("  PR URL        : %s", pr_url)
    logger.info("=" * 60)
    logger.info("Next step: run stage2_merge_and_promote.py with PR_URL=%s", pr_url)


def _format_deviation_summary(report: DeviationReport, profile: dict) -> str:
    lines = [
        f"Tenant `{report.tenant_id}` has {len(report.extra_fields)} non-standard field(s) "
        f"not present in the standard Silver template.",
        "",
        f"**Extra fields:** {', '.join(f'`{f}`' for f in report.extra_fields)}",
    ]
    if report.missing_fields:
        lines.append(f"**Missing fields:** {', '.join(f'`{f}`' for f in report.missing_fields)}")
    if report.type_mismatches:
        lines.append("**Type mismatches:**")
        for m in report.type_mismatches:
            lines.append(f"  - `{m['field']}`: expected `{m['expected']}`, got `{m['actual']}`")
    custom_states = profile.get("custom_workflow_states", [])
    if custom_states:
        lines.append(f"**Custom workflow states:** {', '.join(custom_states)}")
        lines.append(
            "The Silver DQ contract will need these states added to the "
            "`expect_column_values_to_be_in_set` expectation."
        )
    return "\n".join(lines)


def _mock_spark():
    """Returns a minimal Spark mock for dry-run (avoids needing a live cluster)."""
    class _MockSpark:
        def sql(self, query: str):
            logger.debug("[mock spark] sql: %s", query[:80])
            return _MockDF()

    class _MockDF:
        def collect(self):
            return []

        def write(self):
            return self

    return _MockSpark()


if __name__ == "__main__":
    main()
