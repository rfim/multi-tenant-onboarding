# Skill: Promotion Classifier

You are classifying a GitHub pull request into one of five change categories
for a multi-tenant data platform. This file provides the decision tree and
output format.

---

## Decision tree

Work through these questions in order. Stop at the first match.

### 1. Does the PR touch AGENTS.md, promotion/categories.yml, or any *.grants.sql or *.rls.sql file?
Yes -> Category 4 (Governance). Stop.

### 2. Does the PR touch databricks.yml at the root level?
Yes -> Category 4 (Governance). Stop.

### 3. Does the PR touch any file in templates/pipelines/bronze_*, templates/pipelines/silver_*, templates/pipelines/vault_*, dbt/models/bronze/**, dbt/models/silver/**, dbt/models/vault/**, or dbt/models/feature_store/**?
Yes -> Category 3 (Shared layer). Stop.

### 4. Does the PR touch orchestrator/** files other than register_tenant.py or provision_grants.py?
Yes -> Category 3 (Shared layer). Stop.

### 5. Does the PR touch orchestrator/handlers/register_tenant.py or orchestrator/handlers/provision_grants.py?
Yes -> Category 4 (Governance). Stop.

### 6. Does the PR title or body contain the string "[hotfix]" or "[emergency]"?
Yes -> Category 5 (Emergency hotfix). Stop. Add a note that emergency category
requires on-call senior engineer approval and a retroactive review within 48h.

### 7. Does the PR touch files only under dbt/models/gold_*/**, dbt/models/gold_*/**, agents/**, or templates/quality_contracts/**?
Yes -> Check whether the files are scoped to a single tenant_id.
  - If all changed files contain the same tenant_id substring: Category 2 (Tenant model). Stop.
  - If files span multiple tenants: Category 3 (Shared layer). Stop.

### 8. Does the PR touch files only under templates/dashboards/**, orchestrator/handlers/scaffold_dashboards.py, or orchestrator/handlers/activate_tenant.py?
Yes -> Category 1 (Tenant config). Stop.

### 9. Default
If none of the above matched, classify as Category 2 (Tenant model) with
confidence 0.6 and note the ambiguity.

---

## Ambiguity handling

If a PR touches files from multiple categories:
- Classify as the highest-risk category present.
- List all matched categories in the ambiguity_note.
- Recommend splitting the PR if the changes are not causally coupled.

---

## Output format

Output a JSON object exactly matching this schema:

```json
{
  "category_id": <int 1-5>,
  "confidence": <float 0.0-1.0>,
  "ambiguity_note": <string or null>,
  "suggested_test_plan": "<3-5 bullet points describing what to test>"
}
```

Do not output any prose outside the JSON block.
Confidence should reflect how certain you are given the files and PR content.
A file-glob-only match with no ambiguity should be 0.95 or higher.
Any ambiguity should reduce confidence below 0.8.

---

## Suggested test plan format

The suggested_test_plan should be a plain-text list separated by newlines,
not markdown. Example:

```
Run databricks bundle validate in dev and staging environments.
Execute the onboarding integration test suite against the dev catalog.
Verify the new tenant's Gold schema is accessible via the tenant service principal.
Confirm DQ contracts are registered in monitoring.dq_contracts.
Check that no existing tenant's pipeline run history shows failures after deployment.
```

Keep the test plan specific to the change category. A Category 1 change needs
different tests from a Category 3 change. Use the category definitions from
promotion/categories.yml to inform what to test.
