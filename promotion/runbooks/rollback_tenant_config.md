# Rollback Runbook: Tenant Config (Category 1)

**Change category:** 1 — Tenant config changes
**SLA for rollback decision:** 24 hours post-deployment (auto_rollback_window)
**Rollback authority:** Any engineer on the platform team
**Estimated rollback time:** 15-30 minutes

---

## When to use this runbook

Use this runbook when a Category 1 change (new customer registration, grant
provisioning, dashboard cloning, DQ contract instantiation) needs to be
reverted after deployment. Typical triggers:

- DQ failure rate exceeds 5% within 24 hours of tenant activation
- Row count drops more than 20% within 24 hours
- The post-deployment agent has posted a rollback recommendation on the PR
- The on-call engineer has assessed the situation and decided rollback is needed

Do not execute this runbook without explicit authorisation from the on-call
engineer or Data Platform Lead.

---

## Pre-rollback checklist

- [ ] Confirm the failure is caused by the Category 1 change, not an unrelated incident
- [ ] Notify the tenant's account manager before deactivation
- [ ] Record the rollback decision in `monitoring.tenant_change_log`
- [ ] Identify which state in the onboarding state machine to roll back to

---

## Rollback steps

### Step 1: Deactivate the tenant

```sql
-- Run via Databricks SQL warehouse
UPDATE platform_catalog.monitoring.tenant_registry
SET state = 'ROLLBACK', deactivated_at = current_timestamp(), deactivation_reason = '<reason>'
WHERE tenant_id = '<tenant_id>';
```

### Step 2: Revoke grants

Execute the inverse of provision_grants.py. The grant revocation SQL is auto-generated
by the scaffolder and stored in `monitoring.grant_history` for this tenant.

```python
# From the Databricks workspace terminal or notebook:
from orchestrator.handlers.provision_grants import revoke_all_grants
revoke_all_grants(tenant_id='<tenant_id>', catalog='platform_catalog', spark=spark)
```

### Step 3: Drop Gold schema (if desired)

Only drop if the tenant's Gold schema is confirmed empty or the tenant account
is being fully cancelled. Do not drop for a temporary rollback.

```sql
DROP SCHEMA IF EXISTS platform_catalog.gold_<tenant_id> CASCADE;
```

### Step 4: Remove DQ contracts

```sql
UPDATE platform_catalog.monitoring.dq_contracts
SET is_active = false, deactivated_at = current_timestamp()
WHERE tenant_id = '<tenant_id>';
```

### Step 5: Verify RLS policy removal

```sql
-- Verify no rows from this tenant are visible to other service principals
SELECT COUNT(*) FROM platform_catalog.silver.events WHERE tenant_id = '<tenant_id>';
-- Should return 0 after grant revocation (RLS hides the rows)
```

### Step 6: Notify and document

- Post rollback completion to `#platform-alerts`
- Update the GitHub PR with rollback outcome
- Create a follow-up ticket to investigate root cause before re-onboarding

---

## Re-onboarding after rollback

Re-onboarding uses the same `onboard-tenant.yml` workflow with `--resume-from-state`
set to the state that failed. Fix the root cause before re-running.
