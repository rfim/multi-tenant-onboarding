# Rollback Runbook: Governance (Category 4)

**Change category:** 4 — Governance changes
**SLA for rollback decision:** No automated rollback window
**Rollback authority:** Data Platform Lead (mandatory)
**Estimated rollback time:** 1-4 hours

---

## Important

Governance rollbacks (grant changes, RLS policy changes, schema deletions) have
no automated rollback. The Data Platform Lead must authorise and supervise
execution. This runbook must be executed manually; no script may run it.

---

## Scope of impact

Before executing, determine which tenants are affected:

- Grant changes: all tenants whose service principals were modified
- RLS changes: all tenants on the affected table
- Schema deletions: the tenants whose schemas were dropped

---

## Pre-rollback checklist

- [ ] Data Platform Lead has authorised the rollback in writing (Slack or email)
- [ ] All affected tenants have been notified
- [ ] The grant or policy state to restore has been retrieved from `monitoring.grant_history`
- [ ] Legal or compliance has been consulted if the change was compliance-driven

---

## Grant rollback

```sql
-- Retrieve previous grant state
SELECT * FROM platform_catalog.monitoring.grant_history
WHERE change_timestamp > '<deployment_time>'
  AND change_type = 'GRANT'
ORDER BY change_timestamp DESC;

-- Re-apply grants that were revoked
GRANT <privilege> ON <object_type> <object_name> TO `<principal>`;
```

## RLS policy rollback

```sql
-- RLS policies are versioned in monitoring.rls_policy_history
-- Retrieve the previous policy body and re-apply via StatementExecutionAPI
SELECT policy_body FROM platform_catalog.monitoring.rls_policy_history
WHERE table_name = '<table>' AND is_current = false
ORDER BY created_at DESC LIMIT 1;
```

## Schema deletion rollback

Schema deletions cannot be undone. If a schema was dropped, the recovery path is:

1. Identify the last Delta snapshot for each dropped table from Delta history
   (if the table was dropped with PURGE, recovery is not possible).
2. Recreate the schema and restore tables from Delta history.
3. Re-apply all grants.
4. Notify affected tenants that their data is restored.

This is a destructive scenario. If PURGE was used, escalate to the data engineering
leadership for data recovery assessment.
