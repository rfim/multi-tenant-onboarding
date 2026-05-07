"""
Promotion classifier agent.

Called by the onboard-tenant.yml and promote-staging.yml GitHub Actions workflows
on every PR opened against dev, staging, or main.

Reads the changed files from the PR, loads promotion/categories.yml, and asks
the LLM to classify the PR into one of the five change categories. Posts the
result as a PR comment and applies a GitHub label.

This agent never approves or merges PRs. It only comments and labels.
"""

from __future__ import annotations

import logging
from pathlib import Path

import anthropic
import yaml

logger = logging.getLogger(__name__)

CATEGORIES_FILE = Path(__file__).parents[2] / "promotion" / "categories.yml"
SKILL_FILE = Path(__file__).parents[2] / "skills" / "promotion_classifier.md"
MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 2048


def classify(changed_files: list[str], pr_title: str, pr_body: str) -> dict:
    """
    Classifies a PR into a change category.

    Args:
        changed_files: list of file paths changed in the PR
        pr_title: the PR title
        pr_body: the PR description

    Returns:
        dict with keys:
          category_id: int (1-5)
          category_name: str
          confidence: float (0.0-1.0)
          promotion_path: list[str]
          sla_hours: int
          required_approvers: list[dict]
          suggested_test_plan: str
          ambiguity_note: str | None
          human_review_required: bool (always True)
    """
    categories = _load_categories()
    skill = SKILL_FILE.read_text()

    rule_based = _apply_file_glob_rules(changed_files, categories)

    if rule_based["confidence"] >= 0.95:
        logger.info("Rule-based classification succeeded: %s", rule_based["category_name"])
        return rule_based

    logger.info("Rule-based classification ambiguous; calling LLM.")
    return _llm_classify(changed_files, pr_title, pr_body, categories, skill, rule_based)


def _apply_file_glob_rules(changed_files: list[str], categories: dict) -> dict:
    """
    Applies glob pattern rules from categories.yml without calling the LLM.

    Returns a classification with confidence = 1.0 if unambiguous,
    or confidence < 0.95 if multiple categories match.
    """
    import fnmatch

    matched_categories: set[int] = set()

    for cat_name, cat in categories["categories"].items():
        for pattern in cat.get("files_in_scope", []):
            for f in changed_files:
                if fnmatch.fnmatch(f, pattern):
                    matched_categories.add(cat["id"])
                    break

    if not matched_categories:
        # Default to tenant_model for unrecognised files
        return _format_result(categories["categories"]["tenant_model"], 0.5, None)

    if len(matched_categories) == 1:
        cat_id = next(iter(matched_categories))
        cat = _find_category_by_id(categories, cat_id)
        return _format_result(cat, 1.0, None)

    # Multiple categories matched: take the highest-risk one
    highest_id = max(matched_categories)
    cat = _find_category_by_id(categories, highest_id)
    note = (
        f"Files match multiple categories: {sorted(matched_categories)}. "
        f"Classified as Category {highest_id} (highest risk). "
        f"Consider splitting this PR if the changes are not causally coupled."
    )
    return _format_result(cat, 0.7, note)


def _llm_classify(
    changed_files: list[str],
    pr_title: str,
    pr_body: str,
    categories: dict,
    skill: str,
    rule_based: dict,
) -> dict:
    """Falls back to LLM classification when file globs are ambiguous."""
    client = anthropic.Anthropic()

    categories_yaml = yaml.dump(
        {k: {
            "id": v["id"],
            "description": v["description"],
            "files_in_scope": v.get("files_in_scope", []),
        } for k, v in categories["categories"].items()},
        default_flow_style=False,
    )

    message = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=(
            "You are classifying a GitHub pull request into one of five change categories "
            "for a multi-tenant data platform. Follow the skill file exactly. "
            "Output a JSON object only. No prose."
        ),
        messages=[{
            "role": "user",
            "content": (
                f"{skill}\n\n"
                f"## Categories\n\n{categories_yaml}\n\n"
                f"## PR title\n{pr_title}\n\n"
                f"## PR body\n{pr_body}\n\n"
                f"## Changed files\n" + "\n".join(f"- {f}" for f in changed_files) + "\n\n"
                f"## Rule-based pre-classification\n"
                f"Category {rule_based['category_id']} with confidence {rule_based['confidence']}\n\n"
                f"Output JSON: {{\"category_id\": int, \"confidence\": float, "
                f"\"ambiguity_note\": str|null, \"suggested_test_plan\": str}}"
            ),
        }],
    )

    import json
    parsed = json.loads(message.content[0].text)
    cat = _find_category_by_id(categories, parsed["category_id"])

    return {
        **_format_result(cat, parsed["confidence"], parsed.get("ambiguity_note")),
        "suggested_test_plan": parsed.get("suggested_test_plan", ""),
    }


def _load_categories() -> dict:
    return yaml.safe_load(CATEGORIES_FILE.read_text())


def _find_category_by_id(categories: dict, cat_id: int) -> dict:
    for cat in categories["categories"].values():
        if cat["id"] == cat_id:
            return cat
    raise ValueError(f"No category with id {cat_id}")


def _format_result(cat: dict, confidence: float, ambiguity_note: str | None) -> dict:
    return {
        "category_id": cat["id"],
        "category_name": cat["display_name"],
        "confidence": confidence,
        "promotion_path": cat["promotion_path"],
        "sla_hours": cat["sla_hours"],
        "required_approvers": cat.get("required_approvers", []),
        "rollback_runbook": cat.get("rollback_runbook"),
        "suggested_test_plan": "",
        "ambiguity_note": ambiguity_note,
        "human_review_required": True,
        "generated_by": f"promotion_classifier/{MODEL}",
    }
