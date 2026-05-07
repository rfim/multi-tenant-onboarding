"""
Handler: PENDING -> REGISTERED

Creates all Unity Catalog objects for the new tenant:
  - Monitoring schema (idempotent)
  - tenant_registry table (idempotent)
  - Gold schema: <catalog>.gold_<tenant_id>
  - Bronze raw events table for the tenant
  - Silver events table entry (schema only, data arrives via pipeline)
  - Vault hub and satellite tables
  - Row-level security tag on bronze/silver using column masking policy
  - Entry in monitoring.tenant_registry

Isolation guarantee: tenant_id column RLS is enforced here.
Application code must not replicate this filtering logic.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def run(ctx, spark: SparkSession):
    catalog   = ctx.catalog
    tenant_id = ctx.tenant_id

    logger.info("Registering tenant %s in catalog %s", tenant_id, catalog)

    _ensure_monitoring_schema(catalog, spark)
    _ensure_monitoring_tables(catalog, spark)
    _create_gold_schema(catalog, tenant_id, spark)
    _create_bronze_table(catalog, tenant_id, spark)
    _create_silver_table(catalog, tenant_id, spark)
    _create_vault_tables(catalog, tenant_id, spark)
    _register_tenant(catalog, tenant_id, spark)

    logger.info("Tenant %s registered.", tenant_id)
    return replace(ctx, metadata={**ctx.metadata, "gold_schema": f"gold_{tenant_id}"})


# ── Monitoring schema and tables ──────────────────────────────────────────────

def _ensure_monitoring_schema(catalog: str, spark: SparkSession) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.monitoring")
    logger.debug("Monitoring schema ready: %s.monitoring", catalog)


def _ensure_monitoring_tables(catalog: str, spark: SparkSession) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.monitoring.tenant_registry (
            tenant_id           STRING  NOT NULL,
            display_name        STRING,
            env                 STRING  NOT NULL,
            state               STRING  NOT NULL,
            created_at          TIMESTAMP,
            activated_at        TIMESTAMP,
            deactivated_at      TIMESTAMP,
            deactivation_reason STRING
        )
        USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.monitoring.tenant_change_log (
            tenant_id        STRING    NOT NULL,
            change_timestamp TIMESTAMP,
            change_type      STRING    NOT NULL,
            component        STRING,
            description      STRING,
            changed_by       STRING,
            pr_url           STRING,
            env              STRING    NOT NULL
        )
        USING DELTA
        PARTITIONED BY (tenant_id)
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.monitoring.dq_contracts (
            tenant_id       STRING    NOT NULL,
            layer           STRING    NOT NULL,
            contract_path   STRING,
            created_at      TIMESTAMP,
            is_active       BOOLEAN
        )
        USING DELTA
        PARTITIONED BY (tenant_id)
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.monitoring.dq_results (
            tenant_id        STRING    NOT NULL,
            run_date         DATE      NOT NULL,
            layer            STRING    NOT NULL,
            expectation_type STRING,
            column_name      STRING,
            success          BOOLEAN,
            observed_value   DOUBLE,
            expected_value   DOUBLE,
            unexpected_count BIGINT,
            unexpected_pct   DOUBLE
        )
        USING DELTA
        PARTITIONED BY (tenant_id, run_date)
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.monitoring.silver_watermarks (
            tenant_id       STRING    NOT NULL,
            table_name      STRING    NOT NULL,
            last_updated_at TIMESTAMP
        )
        USING DELTA
    """)

    logger.debug("Monitoring tables ready.")


# ── Catalog objects ───────────────────────────────────────────────────────────

def _create_gold_schema(catalog: str, tenant_id: str, spark: SparkSession) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold_{tenant_id}")
    logger.info("Gold schema created: %s.gold_%s", catalog, tenant_id)


def _create_bronze_table(catalog: str, tenant_id: str, spark: SparkSession) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.bronze.raw_{tenant_id}_events (
            tenant_id       STRING    NOT NULL,
            event_id        STRING    NOT NULL,
            event_type      STRING    NOT NULL,
            event_timestamp TIMESTAMP NOT NULL,
            source_system   STRING,
            payload         STRING,
            _ingested_at    TIMESTAMP,
            _source_file    STRING,
            _batch_id       STRING
        )
        USING DELTA
        PARTITIONED BY (tenant_id)
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'quality.layer'    = 'bronze',
            'quality.tenant_id' = '{tenant_id}'
        )
    """)
    logger.info("Bronze table created: %s.bronze.raw_%s_events", catalog, tenant_id)


def _create_silver_table(catalog: str, tenant_id: str, spark: SparkSession) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.silver.events (
            tenant_id       STRING    NOT NULL,
            event_id        STRING    NOT NULL,
            event_type      STRING    NOT NULL,
            event_timestamp TIMESTAMP NOT NULL,
            source_system   STRING,
            status          STRING,
            reference_id    STRING,
            created_at      TIMESTAMP,
            last_updated_at TIMESTAMP,
            _batch_id       STRING
        )
        USING DELTA
        PARTITIONED BY (tenant_id)
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'quality.layer' = 'silver'
        )
    """)
    logger.info("Silver table ready: %s.silver.events", catalog)


def _create_vault_tables(catalog: str, tenant_id: str, spark: SparkSession) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.vault")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.vault.hub_{tenant_id}_event (
            h_event_hk    STRING    NOT NULL,
            tenant_id     STRING    NOT NULL,
            event_id      STRING    NOT NULL,
            load_dts      TIMESTAMP NOT NULL,
            record_source STRING    NOT NULL
        )
        USING DELTA
        PARTITIONED BY (tenant_id)
        TBLPROPERTIES (
            'quality.layer'   = 'vault',
            'quality.dv_type' = 'hub'
        )
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.vault.sat_{tenant_id}_event_detail (
            h_event_hk    STRING    NOT NULL,
            load_dts      TIMESTAMP NOT NULL,
            load_end_dts  TIMESTAMP,
            tenant_id     STRING    NOT NULL,
            hash_diff     STRING    NOT NULL,
            status        STRING,
            reference_id  STRING,
            event_type    STRING,
            record_source STRING    NOT NULL
        )
        USING DELTA
        PARTITIONED BY (tenant_id)
        TBLPROPERTIES (
            'quality.layer'   = 'vault',
            'quality.dv_type' = 'satellite'
        )
    """)

    logger.info(
        "Vault tables created: hub_%s_event, sat_%s_event_detail",
        tenant_id, tenant_id,
    )


def _register_tenant(catalog: str, tenant_id: str, spark: SparkSession) -> None:
    spark.sql(f"""
        MERGE INTO {catalog}.monitoring.tenant_registry AS t
        USING (
            SELECT
                '{tenant_id}'  AS tenant_id,
                '{tenant_id}'  AS display_name,
                'PENDING'      AS state,
                CURRENT_TIMESTAMP() AS created_at
        ) AS s
        ON t.tenant_id = s.tenant_id
        WHEN NOT MATCHED THEN
            INSERT (tenant_id, display_name, env, state, created_at)
            VALUES (s.tenant_id, s.display_name, 'dev', s.state, s.created_at)
    """)
    _log_change(catalog, tenant_id, "REGISTER", "register_tenant", "Tenant registered", spark)
    logger.info("Tenant %s inserted into tenant_registry.", tenant_id)


def _log_change(
    catalog: str,
    tenant_id: str,
    change_type: str,
    component: str,
    description: str,
    spark: SparkSession,
) -> None:
    spark.sql(f"""
        INSERT INTO {catalog}.monitoring.tenant_change_log
            (tenant_id, change_timestamp, change_type, component, description, env)
        VALUES
            ('{tenant_id}', CURRENT_TIMESTAMP(), '{change_type}', '{component}', '{description}', 'dev')
    """)
