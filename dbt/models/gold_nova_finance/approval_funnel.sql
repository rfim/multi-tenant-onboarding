{{
  config(
    materialized = 'table',
    tags         = ['gold', 'daily', 'funnel']
  )
}}

/*
  Weekly approval funnel for tenant: nova_finance
  Shows conversion from submitted -> reviewed -> approved/rejected
*/

WITH weekly AS (
    SELECT
        DATE_TRUNC('week', event_timestamp)     AS week_start,
        status,
        COUNT(DISTINCT reference_id)             AS references,
        COUNT(*)                                 AS events
    FROM {{ source('silver', 'events') }}
    WHERE tenant_id = 'nova-finance'
      AND event_timestamp >= DATE_ADD(CURRENT_DATE(), -90)
    GROUP BY 1, 2
),

pivot AS (
    SELECT
        week_start,
        SUM(CASE WHEN status = 'pending'       THEN references ELSE 0 END) AS submitted,
        SUM(CASE WHEN status = 'in_progress'   THEN references ELSE 0 END) AS in_review,
        SUM(CASE WHEN status = 'pending_audit' THEN references ELSE 0 END) AS pending_audit,
        SUM(CASE WHEN status = 'escalated'     THEN references ELSE 0 END) AS escalated,
        SUM(CASE WHEN status = 'approved'      THEN references ELSE 0 END) AS approved,
        SUM(CASE WHEN status = 'rejected'      THEN references ELSE 0 END) AS rejected,
        SUM(references)                                                      AS total
    FROM weekly
    GROUP BY week_start
)

SELECT
    *,
    ROUND(approved * 100.0 / NULLIF(total, 0), 1)  AS approval_rate_pct,
    ROUND(rejected * 100.0 / NULLIF(total, 0), 1)  AS rejection_rate_pct,
    CURRENT_TIMESTAMP()                              AS refreshed_at
FROM pivot
ORDER BY week_start
