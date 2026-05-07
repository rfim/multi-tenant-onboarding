{{
  config(
    materialized = 'table',
    partition_by = {'field': 'event_date', 'data_type': 'date'},
    tags         = ['gold', 'daily', 'kpi']
  )
}}

/*
  Gold KPI summary for tenant: nova_finance
  Refreshed: daily
  Source: platform_catalog_dev.silver.events WHERE tenant_id = 'nova-finance'
*/

WITH base AS (
    SELECT
        DATE(event_timestamp)              AS event_date,
        event_type,
        status,
        COUNT(*)                           AS event_count,
        COUNT(DISTINCT reference_id)       AS unique_references,
        MIN(event_timestamp)               AS first_event_ts,
        MAX(event_timestamp)               AS last_event_ts
    FROM {{ source('silver', 'events') }}
    WHERE tenant_id = 'nova-finance'
      AND event_timestamp >= DATE_ADD(CURRENT_DATE(), -90)
    GROUP BY 1, 2, 3
),

with_rates AS (
    SELECT
        *,
        ROUND(
            event_count * 100.0 /
            NULLIF(SUM(event_count) OVER (PARTITION BY event_date), 0),
            2
        ) AS pct_of_day
    FROM base
)

SELECT
    event_date,
    event_type,
    status,
    event_count,
    unique_references,
    pct_of_day,
    first_event_ts,
    last_event_ts,
    CURRENT_TIMESTAMP() AS refreshed_at
FROM with_rates
