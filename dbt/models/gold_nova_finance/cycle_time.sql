{{
  config(
    materialized = 'table',
    tags         = ['gold', 'daily', 'cycle_time']
  )
}}

/*
  Cycle time per reference_id for tenant: nova_finance
  A "cycle" is the span from the first event to the last completed/approved event.
*/

WITH events AS (
    SELECT
        reference_id,
        tenant_id,
        event_type,
        status,
        event_timestamp
    FROM {{ source('silver', 'events') }}
    WHERE tenant_id = 'nova-finance'
      AND reference_id IS NOT NULL
),

lifecycle AS (
    SELECT
        reference_id,
        MIN(event_timestamp)                                         AS started_at,
        MAX(CASE WHEN status IN ('completed','approved','rejected')
                 THEN event_timestamp END)                           AS completed_at,
        COUNT(DISTINCT event_type)                                   AS step_count,
        COUNT(*)                                                     AS total_events,
        MAX(CASE WHEN status = 'approved'   THEN 1 ELSE 0 END)      AS was_approved,
        MAX(CASE WHEN status = 'rejected'   THEN 1 ELSE 0 END)      AS was_rejected,
        MAX(CASE WHEN status IN ('pending_audit','escalated')
                 THEN 1 ELSE 0 END)                                  AS was_escalated
    FROM events
    GROUP BY reference_id
)

SELECT
    reference_id,
    started_at,
    completed_at,
    step_count,
    total_events,
    DATEDIFF(completed_at, started_at)           AS cycle_days,
    DATEDIFF(completed_at, started_at) * 24.0    AS cycle_hours,
    was_approved,
    was_rejected,
    was_escalated,
    CASE
        WHEN was_approved  = 1 THEN 'approved'
        WHEN was_rejected  = 1 THEN 'rejected'
        WHEN was_escalated = 1 THEN 'escalated'
        ELSE 'in_progress'
    END                                           AS final_status,
    CURRENT_TIMESTAMP()                           AS refreshed_at
FROM lifecycle
WHERE completed_at IS NOT NULL
