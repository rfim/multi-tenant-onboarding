# Rollback Runbook: Tenant Model (Category 2)

**Change category:** 2 — Tenant-specific model changes
**SLA for rollback decision:** 12 hours post-deployment
**Rollback authority:** Any engineer on the platform team
**Estimated rollback time:** 10-20 minutes

---

## Scope

This rollback affects only the tenant identified in the PR. No other tenants
are impacted by executing this runbook.

---

## Pre-rollback checklist

- [ ] Confirm the regression is in the tenant-specific Gold model, not a shared layer
- [ ] Identify the last known-good version of the affected Gold tables
- [ ] Notify the tenant's account manager

---

## Rollback steps

### Restore Gold tables

```sql
-- Identify the version before the deployment
DESCRIBE HISTORY platform_catalog.gold_<tenant_id>.kpi_summary LIMIT 10;

-- Restore to the previous version
RESTORE TABLE platform_catalog.gold_<tenant_id>.kpi_summary TO VERSION AS OF <version>;
RESTORE TABLE platform_catalog.gold_<tenant_id>.cycle_time TO VERSION AS OF <version>;
```

### Revert dbt model

Revert the Gold model SQL file to the previous version via Git and open a new PR.
Do not delete the model file; revert to the last known-good content.

```bash
git revert <merge-commit-sha> --no-commit
# Review the revert, then push and open a PR labelled tenant_model
```

### Verify DQ contracts pass

After restoring, run the GE expectation suite for the tenant's Gold layer to confirm
the contracts pass.
