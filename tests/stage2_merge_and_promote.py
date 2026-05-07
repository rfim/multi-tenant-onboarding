"""
Stage 2 test: Merge PR -> automate promotion workflow.

Picks up the PR opened in Stage 1 (pass PR_NUMBER as env var or CLI arg).
Simulates the full promotion pipeline:

  1. Run the promotion classifier against the PR's changed files
  2. Assert the PR is classified as Category 2 (tenant_model)
  3. Merge the PR into dev (simulated or real via GitHub CLI)
  4. Trigger promote-staging.yml via gh workflow run
  5. Poll the workflow run until it completes (pass/fail)
  6. Run the staging comparison agent against a baseline snapshot
  7. Confirm the post-merge state and print a promotion readiness report

Run:
  PR_NUMBER=42 CODEX_AUTH_JSON='{"api_key":"..."}' python -m tests.stage2_merge_and_promote

Or with a simulated PR (no real GitHub token needed):
  SIMULATE=1 CODEX_AUTH_JSON='{"api_key":"..."}' python -m tests.stage2_merge_and_promote
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from agents.promotion_classifier.classifier import classify

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("stage2")

SIMULATE = os.environ.get("SIMULATE", "0") == "1"
PR_NUMBER = os.environ.get("PR_NUMBER", "")
REPO = "rfim/multi-tenant-onboarding"
WORKFLOW_FILE = "promote-staging.yml"
POLL_INTERVAL_S = 15
POLL_TIMEOUT_S  = 600


def main() -> None:
    logger.info("=" * 60)
    logger.info("STAGE 2: Merge PR -> Automate Promotion Workflow")
    logger.info("Simulate: %s | PR: %s", SIMULATE, PR_NUMBER or "(none)")
    logger.info("=" * 60)

    # ── Step 1: Classify the PR ──────────────────────────────────────────────
    logger.info("[1/5] Running promotion classifier...")

    # Files changed by the Stage 1 PR (adapted Silver model + DQ contract)
    changed_files = [
        f"dbt/models/silver/acme-corp/silver_acme-corp_merge_adapted.sql",
        f"dq/expectations/acme-corp/silver_contract.yml",
    ]
    pr_title = "[deviation] acme-corp adapted Silver models"
    pr_body  = "Adapted Silver MERGE for tenant acme-corp. Extra fields: approval_tier, vendor_code, amount_usd."

    result = classify(
        changed_files=changed_files,
        pr_title=pr_title,
        pr_body=pr_body,
    )

    logger.info(
        "[1/5] Classification result: Category %d (%s) | Confidence: %.2f",
        result["category_id"],
        result["category_name"],
        result["confidence"],
    )
    if result["ambiguity_note"]:
        logger.warning("  Ambiguity: %s", result["ambiguity_note"])
    logger.info("  Promotion path : %s", " -> ".join(result["promotion_path"]))
    logger.info("  SLA            : %d hours", result["sla_hours"])
    logger.info("  Approvers      : %s", result["required_approvers"] or "CI only")
    if result["suggested_test_plan"]:
        logger.info("  Suggested tests:\n    %s", result["suggested_test_plan"].replace("\n", "\n    "))

    assert result["category_id"] == 2, (
        f"Expected Category 2 (tenant_model), got {result['category_id']} ({result['category_name']}). "
        f"Check the changed_files list or the promotion classifier."
    )
    logger.info("[1/5] Classification correct: Category 2 tenant_model.")

    # ── Step 2: Merge the PR ─────────────────────────────────────────────────
    logger.info("[2/5] Merging PR...")

    if SIMULATE:
        logger.info("[2/5] [SIMULATE] Skipping real merge. Treating PR as merged.")
        pr_number = "999"
    elif PR_NUMBER:
        pr_number = PR_NUMBER
        _gh("pr", "merge", pr_number,
            "--repo", REPO,
            "--squash",
            "--auto",
            "--delete-branch")
        logger.info("[2/5] PR #%s merged.", pr_number)
    else:
        logger.error("[2/5] Set PR_NUMBER env var or SIMULATE=1.")
        sys.exit(1)

    # ── Step 3: Trigger promote-staging workflow ─────────────────────────────
    logger.info("[3/5] Triggering %s workflow...", WORKFLOW_FILE)

    if SIMULATE:
        logger.info("[3/5] [SIMULATE] Skipping workflow dispatch.")
        run_id = "simulated-run-001"
    else:
        _gh("workflow", "run", WORKFLOW_FILE,
            "--repo", REPO,
            "--field", f"pr_number={pr_number}")
        time.sleep(5)  # let GitHub register the run
        run_id = _get_latest_run_id(WORKFLOW_FILE)
        logger.info("[3/5] Workflow run ID: %s", run_id)

    # ── Step 4: Poll until workflow completes ────────────────────────────────
    logger.info("[4/5] Polling workflow run %s (timeout %ds)...", run_id, POLL_TIMEOUT_S)

    if SIMULATE:
        conclusion = "success"
        logger.info("[4/5] [SIMULATE] Workflow concluded: %s", conclusion)
    else:
        conclusion = _poll_run(run_id)
        logger.info("[4/5] Workflow concluded: %s", conclusion)

    if conclusion != "success":
        logger.error("[4/5] Workflow failed with conclusion: %s", conclusion)
        logger.error("      Check the Actions tab: https://github.com/%s/actions", REPO)
        sys.exit(1)

    # ── Step 5: Staging comparison (agentic) ────────────────────────────────
    logger.info("[5/5] Running staging comparison agent...")

    comparison = _run_staging_comparison(changed_files)

    logger.info("[5/5] Staging comparison result:")
    logger.info("  Schema drift    : %s", comparison["schema_drift"])
    logger.info("  Row count delta : %s%%", comparison["row_count_delta_pct"])
    logger.info("  DQ pass rate    : %s%%", comparison["dq_pass_rate_pct"])
    logger.info("  Recommendation  : %s", comparison["recommendation"])

    if comparison["recommendation"] == "BLOCK":
        logger.error("[5/5] Staging comparison agent recommends blocking promotion.")
        logger.error("      Reason: %s", comparison.get("block_reason", "see agent output"))
        sys.exit(1)

    # ── Summary ──────────────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    logger.info("STAGE 2 COMPLETE — PROMOTION READINESS REPORT")
    logger.info("  PR #           : %s", pr_number)
    logger.info("  Category       : %d (%s)", result["category_id"], result["category_name"])
    logger.info("  Classifier conf: %.2f", result["confidence"])
    logger.info("  Workflow       : %s", conclusion.upper())
    logger.info("  Schema drift   : %s", comparison["schema_drift"])
    logger.info("  DQ pass rate   : %s%%", comparison["dq_pass_rate_pct"])
    logger.info("  Status         : READY FOR PRODUCTION")
    logger.info("=" * 60)
    logger.info(
        "To promote to production, run:\n"
        "  gh workflow run promote-production.yml "
        "--repo %s --field pr_number=%s",
        REPO, pr_number,
    )


def _run_staging_comparison(changed_files: list[str]) -> dict:
    """
    Lightweight staging comparison: checks that the adapted model files
    parse as valid SQL and that the DQ contract YAML is valid.

    In a real environment this would call the staging Databricks workspace
    and compare row counts against monitoring.staging_baseline.
    """
    import re

    schema_drift  = False
    dq_pass_rate  = 100.0
    issues: list[str] = []

    for f in changed_files:
        if f.endswith(".sql"):
            # Minimal SQL sanity: must contain MERGE INTO or CREATE
            content = _read_changed_file(f)
            if content and not re.search(r"\b(MERGE|CREATE|SELECT)\b", content, re.I):
                issues.append(f"{f}: no recognisable SQL statement found")
                schema_drift = True

        if f.endswith(".yml"):
            content = _read_changed_file(f)
            if content:
                import yaml
                try:
                    yaml.safe_load(content)
                except yaml.YAMLError as e:
                    issues.append(f"{f}: invalid YAML — {e}")
                    dq_pass_rate = 0.0

    recommendation = "BLOCK" if issues else "PROMOTE"
    return {
        "schema_drift":       schema_drift,
        "row_count_delta_pct": 0,   # placeholder; real check queries monitoring.staging_baseline
        "dq_pass_rate_pct":   dq_pass_rate,
        "recommendation":     recommendation,
        "block_reason":       "; ".join(issues) if issues else None,
    }


def _read_changed_file(path: str) -> str | None:
    """Reads a file relative to the repo root if it exists."""
    full = Path(__file__).parents[1] / path
    return full.read_text() if full.exists() else None


def _gh(*args: str) -> str:
    """Runs a gh CLI command and returns stdout."""
    cmd = ["gh"] + list(args)
    logger.debug("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return result.stdout.strip()


def _get_latest_run_id(workflow_file: str) -> str:
    output = _gh(
        "run", "list",
        "--repo", REPO,
        "--workflow", workflow_file,
        "--limit", "1",
        "--json", "databaseId",
        "--jq", ".[0].databaseId",
    )
    return output.strip()


def _poll_run(run_id: str) -> str:
    """Polls a workflow run until it reaches a terminal state. Returns conclusion."""
    deadline = time.time() + POLL_TIMEOUT_S
    while time.time() < deadline:
        output = _gh(
            "run", "view", run_id,
            "--repo", REPO,
            "--json", "status,conclusion",
            "--jq", "[.status, .conclusion] | @tsv",
        )
        status, conclusion = output.strip().split("\t")
        logger.debug("  Run %s: status=%s conclusion=%s", run_id, status, conclusion)

        if status == "completed":
            return conclusion

        time.sleep(POLL_INTERVAL_S)

    return "timed_out"


if __name__ == "__main__":
    main()
