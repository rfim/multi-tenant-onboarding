"""
Alert writer for the anomaly detector.

Formats anomalies and dispatches them to the appropriate output channel:
  - Sev-1: Slack alert + draft PR (via pr_writer pattern from deviation_detector)
  - Sev-2: Slack alert only
  - Sev-3: monitoring.dq_alerts only

Permitted: write to monitoring.dq_alerts, post to #dq-<tenant_id> and
           #platform-alerts, open draft PRs against dev.
Prohibited: modify existing DQ contracts, write to any Silver or Gold table,
            send to channels other than those listed above.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from agents.anomaly_detector.detector import Anomaly, generate_remediation

logger = logging.getLogger(__name__)


def dispatch(anomaly: Anomaly, catalog: str, slack_channel_prefix: str, spark=None) -> None:
    """
    Routes an anomaly to the appropriate output(s) based on severity.
    """
    _write_to_monitoring(anomaly, catalog, spark)

    if anomaly.severity in ("sev1", "sev2"):
        _post_slack_alert(anomaly, slack_channel_prefix)

    if anomaly.severity == "sev1":
        remediation = generate_remediation(anomaly)
        _open_remediation_pr(anomaly, remediation)


def _write_to_monitoring(anomaly: Anomaly, catalog: str, spark) -> None:
    """Appends a row to monitoring.dq_alerts."""
    if spark is None:
        return
    row = {
        "tenant_id": anomaly.tenant_id,
        "layer": anomaly.layer,
        "metric": anomaly.metric,
        "severity": anomaly.severity,
        "current_value": anomaly.current_value,
        "baseline_value": anomaly.baseline_value,
        "delta_pct": anomaly.delta_pct,
        "description": anomaly.description,
        "detected_at": datetime.now(timezone.utc).isoformat(),
        "resolved": False,
    }
    # TODO: spark.createDataFrame([row]).write.format("delta").mode("append")
    #       .saveAsTable(f"{catalog}.monitoring.dq_alerts")
    logger.debug("Would write alert to monitoring.dq_alerts: %s", row)


def _post_slack_alert(anomaly: Anomaly, prefix: str) -> None:
    """Posts a Slack message to the per-tenant DQ channel."""
    channel = f"{prefix}dq-{anomaly.tenant_id}"
    message = _format_slack_message(anomaly)
    logger.info("Would post to Slack channel %s: %s", channel, message[:120])
    # TODO: POST to Slack API using token from Databricks Secrets
    # Only send to #dq-<tenant_id> or #platform-alerts (sev1 only for platform-alerts)


def _open_remediation_pr(anomaly: Anomaly, remediation: str) -> None:
    """Opens a draft PR proposing new DQ rules to catch this anomaly."""
    # TODO: follow the same pattern as deviation_detector/pr_writer.py
    # Branch: dq-remediation/<tenant_id>/<metric>-<timestamp>
    # Files: add proposed GE expectation to dq/expectations/<tenant_id>/<layer>_contract.yml
    #        or add dbt test to dbt/models/<schema>/<tenant_id>/schema.yml
    # Label: tenant_model (Category 2)
    # Base: dev (never staging or main)
    logger.info(
        "Would open remediation PR for tenant %s, metric %s",
        anomaly.tenant_id, anomaly.metric,
    )


def _format_slack_message(anomaly: Anomaly) -> str:
    emoji = ":rotating_light:" if anomaly.severity == "sev1" else ":warning:"
    return (
        f"{emoji} *DQ {anomaly.severity.upper()} — {anomaly.tenant_id}*\n"
        f"Layer: `{anomaly.layer}` | Metric: `{anomaly.metric}`\n"
        f"Current: `{anomaly.current_value:.2f}` | Baseline: `{anomaly.baseline_value:.2f}` "
        f"| Delta: `{anomaly.delta_pct:+.1f}%`\n"
        f"{anomaly.description}"
    )
