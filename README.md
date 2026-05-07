# Multi-Tenant Data Platform Onboarding

This repository contains the infrastructure-as-code, pipeline templates, agentic layer, and promotion machinery for the B2B SaaS multi-tenant data platform. It is managed as a Databricks Asset Bundle and targets three environments: `dev`, `staging`, and `prod`.

---

## Architecture overview

### Isolation model

Tenants share a Bronze and Silver layer. Tenant isolation within those layers is enforced by a `tenant_id` column with Unity Catalog row-level security policies. Each tenant receives a dedicated Gold schema for analyst-facing models (`gold_<tenant_id>`). The Feature Store is shared across tenants, with feature tables partitioned by `tenant_id`.

```
Unity Catalog
└── platform_catalog
    ├── bronze          (shared, RLS on tenant_id)
    ├── silver          (shared, RLS on tenant_id)
    ├── vault           (shared, RLS on tenant_id)
    ├── gold_<tenant>   (per-tenant schema, one per customer)
    ├── feature_store   (shared, partitioned by tenant_id)
    └── monitoring      (platform telemetry, DQ results, change log)
```

Isolation is enforced at the Unity Catalog layer. Application code does not implement tenant filtering logic; it relies on the RLS policies.

### Component map

| Component | Type | Location |
|---|---|---|
| Onboarding orchestrator | Deterministic state machine | `orchestrator/` |
| Pipeline scaffolder | Template renderer + PR opener | `orchestrator/handlers/scaffold_pipelines.py` |
| Dashboard scaffolder | Template renderer + Superset API | `orchestrator/handlers/scaffold_dashboards.py` |
| Deviation detector | Agentic, PR-only output | `agents/deviation_detector/` |
| Anomaly detector | Agentic, alert + draft PR output | `agents/anomaly_detector/` |
| Changelog summariser | Agentic, Slack/dashboard output | `agents/changelog_summariser/` |
| Promotion classifier | Agentic, PR comment output | `agents/promotion_classifier/` |
| DQ engine | Shared GE + dbt, per-tenant filter | `templates/quality_contracts/` |
| Change log | Append-only Delta table | `monitoring.tenant_change_log` |

---

## Repository structure

```
multi-tenant-onboarding/
├── databricks.yml                    # DAB root manifest
├── orchestrator/
│   ├── state_machine.py              # Deterministic FSM entry point
│   ├── states.py                     # Enum of valid states
│   ├── transitions.py                # Legal state transitions
│   └── handlers/
│       ├── register_tenant.py        # Creates catalog objects + RLS
│       ├── provision_grants.py       # Assigns UC grants
│       ├── scaffold_pipelines.py     # Renders pipeline templates, opens PR
│       ├── scaffold_dashboards.py    # Clones Superset templates
│       ├── scaffold_quality.py       # Instantiates DQ contracts
│       └── activate_tenant.py        # Marks tenant ACTIVE in registry
├── templates/
│   ├── pipelines/
│   │   ├── bronze_ingestion.sql.j2
│   │   ├── silver_merge.sql.j2
│   │   ├── vault_hub.sql.j2
│   │   └── gold_analytics.sql.j2
│   ├── dashboards/
│   │   ├── kpi_overview.json.j2
│   │   ├── approval_volume.json.j2
│   │   ├── cycle_time.json.j2
│   │   ├── vendor_analytics.json.j2
│   │   └── user_activity.json.j2
│   └── quality_contracts/
│       ├── bronze_contract.yml.j2
│       ├── silver_contract.yml.j2
│       └── gold_contract.yml.j2
├── agents/
│   ├── deviation_detector/
│   │   ├── detector.py               # Identifies schema deviations
│   │   └── pr_writer.py              # Drafts adapted Silver models, opens PR
│   ├── anomaly_detector/
│   │   ├── detector.py               # Watches per-tenant DQ history
│   │   └── alert_writer.py           # Proposes alerts + remediations
│   ├── changelog_summariser/
│   │   └── summariser.py             # Generates human-readable summaries
│   └── promotion_classifier/
│       └── classifier.py             # Classifies PR change category
├── skills/
│   ├── deviation_detector.md         # Platform modelling conventions
│   ├── anomaly_detector.md           # DQ patterns and thresholds
│   ├── changelog_summariser.md       # Summary tone and format rules
│   └── promotion_classifier.md       # Change category decision tree
├── promotion/
│   ├── categories.yml                # Five change categories + SLAs
│   ├── canary_tenant.yml             # Canary tenant configuration
│   └── runbooks/
│       ├── rollback_tenant_config.md
│       ├── rollback_tenant_model.md
│       ├── rollback_shared_layer.md
│       ├── rollback_governance.md
│       └── rollback_hotfix.md
├── .github/workflows/
│   ├── onboard-tenant.yml
│   ├── promote-staging.yml
│   ├── promote-canary.yml
│   ├── promote-production.yml
│   └── emergency-hotfix.yml
├── AGENTS.md
└── README.md
```

---

## Onboarding flow

A customer signup webhook hits the `POST /onboard` endpoint, which enqueues a message consumed by the Databricks Workflow `onboarding_orchestrator`. The workflow runs the state machine in `orchestrator/state_machine.py`.

```
PENDING
  └─► REGISTERED          register_tenant.py     — creates catalog schema, RLS policy
        └─► GRANTS_PROVISIONED   provision_grants.py    — assigns UC grants to service principals
              └─► PIPELINES_SCAFFOLDED  scaffold_pipelines.py  — opens PR with pipeline SQL
                    └─► DASHBOARDS_SCAFFOLDED scaffold_dashboards.py — clones Superset templates
                          └─► QUALITY_SCAFFOLDED  scaffold_quality.py    — instantiates DQ contracts
                                └─► ACTIVE              activate_tenant.py     — marks tenant live
```

Each handler writes a record to `monitoring.tenant_change_log` on entry and on completion. Any handler failure transitions the machine to `FAILED` and pages the on-call engineer. The machine does not retry automatically; a human must inspect the failure, fix the root cause, and replay from the failed state.

---

## Graduated promotion

Five change categories govern how code moves from dev to production. Full definitions are in `promotion/categories.yml`.

| Category | Path | SLA | Approvers |
|---|---|---|---|
| Tenant config | dev → staging → prod | 1 hour | CI only |
| Tenant model | dev → staging → prod | 24 hours | 1 engineer |
| Shared layer | dev → staging → canary → prod | 5 days | 2 engineers |
| Governance | dev → staging → prod (7-day hold) | 10 days | Senior + Data Platform Lead |
| Emergency hotfix | direct to prod | 30 minutes | On-call senior engineer |

Rollback runbooks for each category live in `promotion/runbooks/`.

---

## Agentic layer

All agents follow the rule defined in `AGENTS.md`: **AI proposes, tests validate, humans approve, CI/CD deploys.**

| Agent | Trigger | Output |
|---|---|---|
| Deviation detector | Schema mismatch during onboarding | Draft PR with adapted Silver models |
| Anomaly detector | Scheduled DQ result scan | Slack alert + draft PR (Sev-1 only) |
| Changelog summariser | Scheduled (daily/weekly) | Slack post + dashboard update |
| Promotion classifier | PR opened against main | PR comment with category + approvers |

No agent may merge a PR, deploy to production, modify Unity Catalog grants, or execute DDL directly.

---

## Local development

### Prerequisites

- Python 3.11+
- Databricks CLI v0.200+
- `databricks bundle validate` passes
- `DATABRICKS_HOST` and `DATABRICKS_TOKEN` set in environment

### Setup

```bash
pip install -r requirements.txt
databricks bundle validate
databricks bundle deploy --target dev
```

### Running the orchestrator locally

```bash
python -m orchestrator.state_machine \
  --tenant-id acme-corp \
  --dry-run
```

`--dry-run` validates the state machine transitions and renders templates without writing to the catalog or opening PRs.

---

## Unity Catalog conventions

- Catalog name: `platform_catalog` in production, `platform_catalog_dev` in development
- Bronze tables: `platform_catalog.bronze.<source>` — append-only, raw ingest
- Silver tables: `platform_catalog.silver.<entity>` — MERGE logic, type-safe
- Vault tables: `platform_catalog.vault.<hub|link|satellite>` — Data Vault 2.0
- Gold schemas: `platform_catalog.gold_<tenant_id>.<model>` — analyst-facing
- All tables include `tenant_id STRING NOT NULL` as a partition column
- RLS policies are defined in the `register_tenant` handler and must not be replicated in application code

---

## Contributing

Before opening a PR, read `AGENTS.md` and `promotion/categories.yml` to determine the correct change category and required approvers for your change. The promotion classifier agent will add a comment to your PR within a few minutes of opening.

All documentation is written in British English.
