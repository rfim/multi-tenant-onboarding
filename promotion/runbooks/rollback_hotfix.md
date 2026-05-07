# Rollback Runbook: Emergency Hotfix (Category 5)

**Change category:** 5 — Emergency hotfix
**SLA for rollback decision:** 4 hours post-deployment
**Rollback authority:** On-call senior engineer
**Estimated rollback time:** 10-30 minutes

---

## Context

This runbook applies to emergency hotfixes deployed directly to production
via the `emergency-hotfix.yml` workflow. Because these changes bypass the
standard promotion path, the rollback procedure is also expedited.

The retroactive review (due within 48 hours of deployment) should determine
whether the hotfix introduced any regressions and whether this rollback is needed.

---

## Rollback steps

The appropriate rollback procedure depends on what the hotfix changed. Consult
the original hotfix PR to determine the affected layer, then follow the
corresponding runbook:

- SQL changes to Bronze/Silver/Vault: follow `rollback_shared_layer.md` Steps 2-5
- Gold model changes: follow `rollback_tenant_model.md`
- Grant or RLS changes: follow `rollback_governance.md`
- Orchestrator code changes: redeploy the previous bundle version

```bash
# Redeploy previous bundle version
git checkout <previous-sha>
databricks bundle deploy --target prod
```

---

## Post-rollback

A hotfix rollback is a significant event. Complete the retroactive review
immediately rather than waiting the full 48 hours. Determine:

1. Did the hotfix cause a regression in addition to the original incident?
2. Is the original incident resolved, or does the rollback re-open it?
3. What is the correct Category 1-4 path for the permanent fix?

Record all findings in the retroactive review issue opened by the
`emergency-hotfix.yml` workflow.
