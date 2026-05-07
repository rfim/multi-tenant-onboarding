"""
DQ anomaly detector agent.

Runs on a schedule (06:00 UTC daily). For each active tenant, reads the last
30 days of DQ results from monitoring.dq_results and identifies anomalies
that the static GE contracts would not catch:

  - Relative volume changes (vs 7-day rolling average)
  - Format drift (null rate increasing over time)
  - Slow degradation (metric trending outside its historical range)
  - Inter-tenant anomalies (a tenant diverging from the platform baseline)

Output:
  - Sev-1: Slack alert + draft PR proposing new GE expectations or dbt tests
  - Sev-2: Slack alert only
  - Sev-3: monitoring.dq_alerts record only (no Slack)

This agent never modifies existing DQ contracts. It only proposes additions.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import anthropic
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

SKILL_FILE = Path(__file__).parents[2] / "skills" / "anomaly_detector.md"
MODEL = "claude-opus-4-7"
MAX_TOKENS = 4096

Severity = Literal["sev1", "sev2", "sev3"]


@dataclass
class Anomaly:
    tenant_id: str
    layer: str
    metric: str
    current_value: float
    baseline_value: float
    delta_pct: float
    severity: Severity
    description: str
    proposed_remediation: str | None = None


def run(catalog: str, spark: SparkSession) -> list[Anomaly]:
    """
    Entry point. Scans all active tenants and returns all anomalies found.
    """
    active_tenants = _load_active_tenants(catalog, spark)
    all_anomalies = []

    for tenant_id in active_tenants:
        logger.info("Scanning DQ history for tenant %s", tenant_id)
        history = _load_dq_history(catalog, tenant_id, spark)
        anomalies = _detect_anomalies(tenant_id, history)
        all_anomalies.extend(anomalies)

    logger.info("Anomaly scan complete. %d anomalies found.", len(all_anomalies))
    return all_anomalies


def generate_remediation(anomaly: Anomaly) -> str:
    """
    Calls the LLM to propose a concrete remediation for a Sev-1 anomaly.

    Returns a plain-language description of the proposed dbt test or
    GE expectation that would catch this anomaly in future runs.
    """
    skill = SKILL_FILE.read_text()
    client = anthropic.Anthropic()

    message = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=(
            "You are a data quality engineer proposing new monitoring rules "
            "for a multi-tenant Databricks platform. Follow the skill file. "
            "Propose specific, testable dbt tests or Great Expectations expectations. "
            "Do not suggest changes to shared platform infrastructure."
        ),
        messages=[{
            "role": "user",
            "content": f"{skill}\n\n## Anomaly\n\n{_anomaly_to_prompt(anomaly)}"
        }],
    )

    return message.content[0].text


def _load_active_tenants(catalog: str, spark: SparkSession) -> list[str]:
    """Returns all tenant IDs with state = ACTIVE."""
    df = spark.sql(
        f"SELECT tenant_id FROM {catalog}.monitoring.tenant_registry "
        f"WHERE state = 'ACTIVE'"
    )
    return [row.tenant_id for row in df.collect()]


def _load_dq_history(catalog: str, tenant_id: str, spark: SparkSession) -> list[dict]:
    """Loads the last 30 days of DQ run results for a tenant."""
    df = spark.sql(f"""
        SELECT
            run_date,
            layer,
            expectation_type,
            column_name,
            success,
            observed_value,
            expected_value,
            unexpected_count,
            unexpected_pct
        FROM {catalog}.monitoring.dq_results
        WHERE tenant_id = '{tenant_id}'
          AND run_date >= DATE_ADD(current_date(), -30)
        ORDER BY layer, expectation_type, run_date
    """)
    return [row.asDict() for row in df.collect()]


def _detect_anomalies(tenant_id: str, history: list[dict]) -> list[Anomaly]:
    """
    Applies statistical rules to detect anomalies in DQ history.

    Rules applied:
      - Null rate for a key column > 20%: Sev-1
      - Record volume delta > 50% vs 7-day average: Sev-1
      - Null rate trending up over 7 consecutive days: Sev-2
      - unexpected_pct for any expectation > 10%: Sev-2
      - unexpected_pct increasing over 14 days: Sev-3
    """
    # TODO: implement statistical checks
    # Group history by (layer, expectation_type, column_name)
    # For each group: compute rolling statistics and compare to thresholds
    return []


def _anomaly_to_prompt(anomaly: Anomaly) -> str:
    return (
        f"Tenant: {anomaly.tenant_id}\n"
        f"Layer: {anomaly.layer}\n"
        f"Metric: {anomaly.metric}\n"
        f"Current value: {anomaly.current_value}\n"
        f"7-day baseline: {anomaly.baseline_value}\n"
        f"Delta: {anomaly.delta_pct:.1f}%\n"
        f"Description: {anomaly.description}\n"
    )
