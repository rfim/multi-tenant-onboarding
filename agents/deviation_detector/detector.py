"""
Deviation detector agent.

Triggered when scaffold_pipelines detects that a tenant's source schema does
not match the standard template field list. This agent:

  1. Reads the actual schema from the source system or metadata API.
  2. Computes the diff against the standard Silver model field list.
  3. Calls the LLM (via the Anthropic SDK) with the diff and the platform
     modelling skill file to produce adapted Silver model SQL.
  4. Passes the adapted SQL to pr_writer.py to open a draft PR.

This agent never writes SQL to staging or main, never executes DDL, and never
merges PRs. It only produces a draft PR on the dev branch.

LLM guidance is loaded from skills/deviation_detector.md at runtime.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import anthropic

logger = logging.getLogger(__name__)

SKILL_FILE = Path(__file__).parents[2] / "skills" / "deviation_detector.md"
MODEL = "claude-opus-4-7"
MAX_TOKENS = 8192


@dataclass
class DeviationReport:
    tenant_id: str
    standard_fields: list[str]
    actual_fields: list[str]
    extra_fields: list[str]     # in actual but not in standard
    missing_fields: list[str]   # in standard but not in actual
    type_mismatches: list[dict] # [{"field": ..., "expected": ..., "actual": ...}]


def detect(tenant_id: str, actual_schema: dict) -> DeviationReport:
    """
    Computes the deviation between the actual schema and the standard template.

    Args:
        tenant_id: the tenant being onboarded
        actual_schema: dict mapping field_name -> {"type": str, "nullable": bool}

    Returns:
        DeviationReport describing all deviations found.
    """
    standard_fields = _load_standard_fields()
    actual_fields = list(actual_schema.keys())

    extra_fields = [f for f in actual_fields if f not in standard_fields]
    missing_fields = [f for f in standard_fields if f not in actual_fields]
    type_mismatches = _find_type_mismatches(actual_schema)

    return DeviationReport(
        tenant_id=tenant_id,
        standard_fields=standard_fields,
        actual_fields=actual_fields,
        extra_fields=extra_fields,
        missing_fields=missing_fields,
        type_mismatches=type_mismatches,
    )


def generate_adapted_models(report: DeviationReport, catalog: str) -> dict[str, str]:
    """
    Calls the LLM to generate adapted Silver model SQL for the deviating tenant.

    Args:
        report: DeviationReport from detect()
        catalog: Unity Catalog name

    Returns:
        Dict mapping model_filename -> sql_content
    """
    skill = SKILL_FILE.read_text()
    prompt = _build_prompt(report, catalog, skill)

    client = anthropic.Anthropic()

    logger.info(
        "Calling LLM to generate adapted models for tenant %s (%d extra, %d missing fields)",
        report.tenant_id, len(report.extra_fields), len(report.missing_fields),
    )

    message = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=(
            "You are a data platform engineer generating Silver layer SQL models "
            "for a multi-tenant Databricks platform. Follow the skill file instructions "
            "precisely. Output only valid SQL. Do not include explanatory prose outside "
            "of SQL comments."
        ),
        messages=[{"role": "user", "content": prompt}],
    )

    raw_output = message.content[0].text
    return _parse_model_output(raw_output)


def _load_standard_fields() -> list[str]:
    """Returns the field list encoded in the standard Silver template."""
    # TODO: parse templates/pipelines/silver_merge.sql.j2 to extract field names
    # For now, return the known standard set
    return [
        "tenant_id", "event_id", "event_type", "event_timestamp",
        "source_system", "status", "reference_id",
        "created_at", "last_updated_at", "_batch_id",
    ]


def _find_type_mismatches(actual_schema: dict) -> list[dict]:
    """Identifies fields with unexpected types compared to Silver expectations."""
    expected_types = {
        "tenant_id": "STRING",
        "event_id": "STRING",
        "event_type": "STRING",
        "event_timestamp": "TIMESTAMP",
        "status": "STRING",
        "reference_id": "STRING",
    }
    mismatches = []
    for field, expected_type in expected_types.items():
        if field in actual_schema:
            actual_type = actual_schema[field].get("type", "UNKNOWN").upper()
            if actual_type != expected_type:
                mismatches.append({
                    "field": field,
                    "expected": expected_type,
                    "actual": actual_type,
                })
    return mismatches


def _build_prompt(report: DeviationReport, catalog: str, skill: str) -> str:
    return f"""
{skill}

---

## Task

Generate an adapted Silver MERGE model for tenant `{report.tenant_id}`.

The tenant's source schema deviates from the standard template as follows:

**Extra fields (present in source, not in standard template):**
{json.dumps(report.extra_fields, indent=2)}

**Missing fields (in standard template, absent from source):**
{json.dumps(report.missing_fields, indent=2)}

**Type mismatches:**
{json.dumps(report.type_mismatches, indent=2)}

Catalog: `{catalog}`
Gold schema: `gold_{report.tenant_id}`

Produce a single SQL file named `silver_{report.tenant_id}_merge_adapted.sql`.
For each extra field, add it to the Silver MERGE with appropriate casting.
For each missing field, add it as NULL with the expected type and a SQL comment
explaining it is absent from the source.
For each type mismatch, add an explicit CAST and a SQL comment noting the
source type.

Output format:
```sql
-- filename: silver_{report.tenant_id}_merge_adapted.sql
<sql content>
```
"""


def _parse_model_output(raw: str) -> dict[str, str]:
    """Extracts SQL file content from the LLM output."""
    models = {}
    lines = raw.split("\n")
    current_file = None
    buffer = []
    in_block = False

    for line in lines:
        if line.startswith("```sql"):
            in_block = True
            buffer = []
        elif line.startswith("```") and in_block:
            in_block = False
            if current_file:
                models[current_file] = "\n".join(buffer)
        elif in_block:
            if line.startswith("-- filename:"):
                current_file = line.split(":", 1)[1].strip()
            else:
                buffer.append(line)

    return models
