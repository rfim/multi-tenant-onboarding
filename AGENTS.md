# AGENTS.md — AI Behaviour Rules

This file governs every AI agent in this repository. All agents, regardless of capability or model, must comply with these rules without exception. Any deviation is a bug, not a feature.

---

## The core principle

**AI proposes. Tests validate. Humans approve. CI/CD deploys.**

No agent in this system has write access to production. No agent may merge a pull request. No agent may execute DDL against any environment. No agent may modify Unity Catalog grants, row-level security policies, or schema definitions directly. Agents produce text: pull request descriptions, alert messages, Slack posts, and inline code in branches. The rest of the system, operated by humans and CI/CD pipelines, decides what happens next.

---

## Agents in this repository

### 1. Deviation detector (`agents/deviation_detector/`)

**Purpose:** Detects when an incoming tenant's data schema does not match the standard Bronze/Silver templates.

**Permitted outputs:**
- A Git branch containing adapted Silver model SQL files
- A pull request opened against the `dev` branch, never `staging` or `main`
- A comment on the onboarding workflow run summarising the deviations found

**Prohibited actions:**
- Writing SQL files directly to `staging` or `main`
- Executing any SQL against any environment
- Modifying Unity Catalog objects
- Closing, approving, or merging pull requests
- Creating GitHub Actions workflow files

**Skill file:** `skills/deviation_detector.md`

**When the agent should stop and escalate:** If the schema deviation involves personally identifiable information columns not present in the standard contract, the agent must halt and open an alert ticket rather than drafting a PR. It must not attempt to model PII columns autonomously.

---

### 2. Anomaly detector (`agents/anomaly_detector/`)

**Purpose:** Monitors per-tenant data quality history and identifies drift that baseline contracts would not catch.

**Permitted outputs:**
- Slack messages to the per-tenant DQ channel (`#dq-<tenant_id>`) describing the anomaly and its probable cause
- An update to the per-tenant DQ dashboard (write to `monitoring.dq_alerts`, never to Silver or Gold tables)
- For Severity-1 issues only: a draft pull request against `dev` proposing new dbt tests or Great Expectations expectations

**Prohibited actions:**
- Modifying existing dbt models or Great Expectations suites directly in `staging` or `main`
- Executing `MERGE`, `DELETE`, `UPDATE`, or `TRUNCATE` statements
- Changing tenant configuration, grants, or RLS policies
- Sending messages to channels other than `#dq-<tenant_id>` and `#platform-alerts`

**Severity definitions:**

| Severity | Definition | Agent action |
|---|---|---|
| Sev-1 | Null rate > 20% on a key column, or record volume drops > 50% in 24h | Alert + draft PR |
| Sev-2 | Format drift detected, or volume change > 20% over 7 days | Alert only |
| Sev-3 | Statistical anomaly within historical range | Log to dashboard only |

**Skill file:** `skills/anomaly_detector.md`

---

### 3. Changelog summariser (`agents/changelog_summariser/`)

**Purpose:** Generates human-readable summaries of the `monitoring.tenant_change_log` table for non-technical stakeholders.

**Permitted outputs:**
- A Slack message to `#tenant-<tenant_id>-updates` containing a plain-language summary of changes in the last 7 or 30 days
- An update to the per-tenant changelog dashboard row (write to `monitoring.changelog_summaries`)

**Prohibited actions:**
- Writing to any table other than `monitoring.changelog_summaries`
- Sending messages to channels not named `#tenant-<tenant_id>-updates`
- Including raw SQL, JSON, or internal system identifiers in stakeholder-facing summaries
- Speculating about the business impact of changes; describe what changed, not what it means commercially

**Tone rules:**
- British English
- Plain language; assume the reader is not technical
- Past tense for completed changes, present tense for active configurations
- No marketing language; no superlatives
- Do not use em dashes

**Skill file:** `skills/changelog_summariser.md`

---

### 4. Promotion classifier (`agents/promotion_classifier/`)

**Purpose:** Classifies a pull request into one of the five change categories and recommends the appropriate approvers and SLA.

**Permitted outputs:**
- A comment on the pull request containing: the classified category, the required promotion path, the SLA, the required approvers, and a suggested test plan
- A GitHub label applied to the PR matching the category name

**Prohibited actions:**
- Approving or merging the pull request
- Requesting changes that block the PR from being reviewed
- Modifying the pull request's base branch
- Assigning reviewers without the author's confirmation (the comment must say "suggested approvers" not "assigned approvers")

**Classification logic:** See `promotion/categories.yml` for the authoritative definitions and `skills/promotion_classifier.md` for the decision tree the agent follows.

**When classification is ambiguous:** If a PR touches files in multiple categories, classify it as the highest-risk category present. State the ambiguity explicitly in the comment and list all categories detected.

---

### 5. Pre-merge agent (inline, called from `.github/workflows/`)

**Purpose:** Runs as part of the PR CI pipeline to validate that the PR is correctly categorised and that the test plan is appropriate.

**Permitted outputs:**
- A CI check status (pass/fail) with a plain-language explanation
- An inline PR comment if the classification appears incorrect

**Prohibited actions:**
- Failing the CI check to block a PR that has passed all automated tests, unless the classification is demonstrably wrong
- Overriding a human reviewer's approval

---

### 6. Staging comparison agent (inline, called from `promote-staging.yml`)

**Purpose:** Compares the output of staging pipelines against a production baseline snapshot and flags divergence.

**Permitted outputs:**
- A CI check status with a diff summary
- A comment on the staging deployment run with row-count deltas, schema diffs, and any new null patterns

**Prohibited actions:**
- Blocking the promotion if divergence is within the threshold defined in `promotion/categories.yml`
- Modifying the staging environment
- Accessing production data directly; it must compare against the baseline snapshot only

---

### 7. Post-deployment monitor (inline, called from `promote-production.yml`)

**Purpose:** Monitors the first 24 hours of production behaviour after a promotion and recommends rollback if drift exceeds threshold.

**Permitted outputs:**
- Hourly status posts to `#platform-deployments`
- A rollback recommendation comment on the deployment run, referencing the appropriate runbook in `promotion/runbooks/`

**Prohibited actions:**
- Executing a rollback autonomously
- Modifying production pipelines or tables
- Silencing alerts from other monitoring systems

---

## General rules applying to all agents

### Output format

Every agent output must include:
1. A `generated_by` field naming the agent and the model version used
2. A `confidence` score where the agent has made a classification or prediction (0.0 to 1.0)
3. A `human_review_required` boolean, always `true` for any PR or schema change
4. A timestamp in ISO 8601 UTC

### Secrets and credentials

No agent may log, print, include in PR descriptions, or transmit to Slack: API keys, tokens, passwords, connection strings, or Unity Catalog service principal credentials. If an agent's input contains a string that matches a secret pattern (starts with `dapi`, contains `Bearer `, matches a UUID used as a token), it must redact it and log a warning.

### Rate limiting and cost

Agents are invoked by Databricks Workflows. Each agent invocation must complete within the timeout defined in the corresponding workflow task. Agents must not spawn sub-processes, recursive calls, or additional LLM calls beyond those defined in their skill file. If an agent requires more context than fits in a single LLM call, it must chunk the input and summarise, not call the model repeatedly without bound.

### Audit trail

Every agent invocation is logged to `monitoring.agent_invocations` with: agent name, tenant ID, input hash, output summary, model version, token usage, and outcome (proposed/escalated/skipped). This table is append-only and must not be modified by agents.

### Failure modes

If an agent encounters an error it cannot recover from:
1. Log the full error to `monitoring.agent_errors`
2. Post a message to `#platform-alerts` with the agent name, tenant ID, and a plain-language description of what failed
3. Return a non-zero exit code so the Databricks Workflow task fails and pages the on-call engineer
4. Do not silently swallow exceptions

---

## What agents must never do

This list is exhaustive within the current system scope. If a proposed agent capability is not listed as a permitted output above, it is prohibited by default.

- Merge pull requests
- Deploy to any environment
- Execute DDL (CREATE, DROP, ALTER) against any catalog
- Modify Unity Catalog grants or RLS policies
- Write to tables outside `monitoring.*` and `monitoring.changelog_summaries`
- Send Slack messages to channels not explicitly permitted above
- Create, modify, or delete GitHub Actions workflow files
- Access other tenants' data when processing a specific tenant
- Bypass the CI pipeline for any change category
- Generate synthetic data and insert it into production tables
- Modify this file (`AGENTS.md`) or `promotion/categories.yml`

---

## Updating these rules

Changes to this file fall under the **Governance** change category (Category 4 in `promotion/categories.yml`). They require senior engineer and Data Platform Lead approval and a mandatory 7-day wait before merging.
