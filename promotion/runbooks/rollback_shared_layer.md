# Rollback Runbook: Shared Layer (Category 3)

**Change category:** 3 — Shared-layer changes
**SLA for rollback decision:** 48 hours post-deployment
**Rollback authority:** On-call senior engineer + Data Platform Lead sign-off
**Estimated rollback time:** 30-90 minutes depending on Delta history depth

---

## When to use this runbook

Shared-layer rollbacks affect all tenants simultaneously. Use this runbook when:

- The post-deployment agent has recommended rollback with Sev-1 evidence
- DQ failure rate across more than one tenant exceeds 2% after deployment
- Row count delta exceeds 10% vs the production baseline for any tenant
- Schema drift is detected in Silver or Bronze tables after deployment

Shared-layer rollbacks are high-impact. Confirm the rollback decision with
the Data Platform Lead before executing.

---

## Pre-rollback checklist

- [ ] Confirm the regression is caused by the Category 3 change
- [ ] Identify the last known-good Delta table version for each affected layer
- [ ] Ensure all active pipelines that write to the shared layer are stopped
- [ ] Notify all tenant account managers (via the platform comms template)
- [ ] Record rollback decision in `monitoring.tenant_change_log` for all tenants

---

## Step 1: Stop all shared-layer pipeline jobs

```bash
# Via Databricks CLI
databricks jobs list --output JSON | \
  python -m scripts.stop_jobs_by_prefix --prefix "[prod] Bronze" --prefix "[prod] Silver"
```

Do not proceed to Step 2 until all jobs have terminated.

---

## Step 2: Identify rollback version

```sql
-- Find the last version before the deployment for each affected table
DESCRIBE HISTORY platform_catalog.silver.events LIMIT 20;
-- Identify the version timestamped before the deployment time recorded in
-- monitoring.tenant_change_log.
```

Record the target version for each table:

| Table | Target version |
|---|---|
| bronze.raw_events | |
| silver.events | |
| vault.hub_event | |

---

## Step 3: Restore Delta tables

```sql
-- Restore silver.events to the last known-good version
RESTORE TABLE platform_catalog.silver.events TO VERSION AS OF <version>;

-- Repeat for each affected table
RESTORE TABLE platform_catalog.bronze.raw_events TO VERSION AS OF <version>;
RESTORE TABLE platform_catalog.vault.hub_event TO VERSION AS OF <version>;
```

RESTORE is a reversible operation; the rolled-back versions remain in Delta history.

---

## Step 4: Revert the bundle deployment

```bash
# Identify the previous bundle deployment tag
databricks bundle deploy --target prod --ref <previous-git-sha>
```

The previous Git SHA is recorded in `monitoring.tenant_change_log` alongside
the deployment record.

---

## Step 5: Restart pipelines

Restart the shared-layer pipelines and verify DQ contracts pass before
confirming rollback is complete.

---

## Step 6: Document and follow up

- Post rollback completion to `#platform-alerts` and `#platform-deployments`
- Update the GitHub PR with rollback evidence and the root cause assessment
- Open a follow-up issue with the label `shared_layer` and `post-incident`
- Schedule a post-incident review within 72 hours
