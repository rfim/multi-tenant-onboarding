"""
Changelog summariser agent.

Reads the last N days of monitoring.tenant_change_log for each active tenant
and generates a plain-language summary for non-technical stakeholders.

Runs on a weekly schedule (Monday 08:00 UTC). Output:
  - Slack post to #tenant-<tenant_id>-updates
  - Row written to monitoring.changelog_summaries (displayed on the
    per-tenant changelog dashboard)

Tone: British English, plain language, past tense for completed changes,
no technical identifiers, no marketing language. See skills/changelog_summariser.md.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from llm.client import chat, CODEX_MEDIUM

logger = logging.getLogger(__name__)

SKILL_FILE = Path(__file__).parents[2] / "skills" / "changelog_summariser.md"
MODEL = CODEX_MEDIUM
MAX_TOKENS = 2048


def run(catalog: str, lookback_days: int, spark: SparkSession, slack_channel_prefix: str) -> None:
    """
    Entry point. Summarises changelogs for all active tenants.
    """
    active_tenants = _load_active_tenants(catalog, spark)

    for tenant_id in active_tenants:
        logger.info("Summarising changelog for tenant %s (last %d days)", tenant_id, lookback_days)
        changes = _load_changes(catalog, tenant_id, lookback_days, spark)

        if not changes:
            logger.info("No changes found for tenant %s in last %d days.", tenant_id, lookback_days)
            continue

        summary = _generate_summary(tenant_id, changes, lookback_days)
        _write_to_monitoring(catalog, tenant_id, summary, lookback_days, spark)
        _post_to_slack(tenant_id, summary, slack_channel_prefix)


def _load_changes(catalog: str, tenant_id: str, lookback_days: int, spark: SparkSession) -> list[dict]:
    """Loads recent change log entries for a tenant."""
    df = spark.sql(f"""
        SELECT
            change_timestamp,
            change_type,
            component,
            description,
            changed_by,
            pr_url
        FROM {catalog}.monitoring.tenant_change_log
        WHERE tenant_id = '{tenant_id}'
          AND change_timestamp >= DATE_ADD(current_timestamp(), -{lookback_days})
        ORDER BY change_timestamp DESC
        LIMIT 200
    """)
    return [row.asDict() for row in df.collect()]


def _generate_summary(tenant_id: str, changes: list[dict], lookback_days: int) -> str:
    """Calls the LLM to produce a plain-language summary."""
    skill = SKILL_FILE.read_text()
    changes_text = _format_changes_for_prompt(changes)

    response = chat(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=(
            "You are writing a brief platform update for non-technical business stakeholders. "
            "Follow the skill file precisely. Use British English. Plain language only. "
            "No SQL, no JSON, no internal identifiers. No em dashes."
        ),
        messages=[{
            "role": "user",
            "content": (
                f"{skill}\n\n"
                f"## Task\n\n"
                f"Summarise the following platform changes for tenant `{tenant_id}` "
                f"over the last {lookback_days} days. "
                f"Write 3-5 sentences in plain English.\n\n"
                f"## Changes\n\n{changes_text}"
            ),
        }],
    )

    return response.content


def _format_changes_for_prompt(changes: list[dict]) -> str:
    lines = []
    for c in changes:
        ts = c.get("change_timestamp", "unknown time")
        component = c.get("component", "unknown component")
        description = c.get("description", "")
        lines.append(f"- {ts}: [{component}] {description}")
    return "\n".join(lines)


def _write_to_monitoring(
    catalog: str,
    tenant_id: str,
    summary: str,
    lookback_days: int,
    spark: SparkSession,
) -> None:
    """Writes the generated summary to monitoring.changelog_summaries."""
    row = {
        "tenant_id": tenant_id,
        "summary": summary,
        "lookback_days": lookback_days,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model": MODEL,
    }
    # TODO: spark.createDataFrame([row]).write.format("delta").mode("append")
    #       .saveAsTable(f"{catalog}.monitoring.changelog_summaries")
    logger.debug("Would write summary for tenant %s", tenant_id)


def _post_to_slack(tenant_id: str, summary: str, prefix: str) -> None:
    """Posts the summary to the tenant's Slack channel."""
    channel = f"{prefix}tenant-{tenant_id}-updates"
    logger.info("Would post changelog summary for tenant %s to %s", tenant_id, channel)
    # TODO: POST to Slack API with formatted message
    # Message must not contain raw SQL, internal IDs, or technical terms
    # that would confuse a non-technical reader.


def _load_active_tenants(catalog: str, spark: SparkSession) -> list[str]:
    df = spark.sql(
        f"SELECT tenant_id FROM {catalog}.monitoring.tenant_registry WHERE state = 'ACTIVE'"
    )
    return [row.tenant_id for row in df.collect()]
