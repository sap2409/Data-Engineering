# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup (MSEG lead time)
# MAGIC Creates schema and shared table names. Demo only — not real GSK data.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "mseg_lead_time")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"USE SCHEMA {schema}")
    print(f"Using Unity Catalog: {catalog}.{schema}")
except Exception:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    spark.sql(f"USE {schema}")
    catalog = "hive_metastore"
    print(f"Using Hive metastore: {schema}")

# COMMAND ----------

TABLES = {
    "bronze_mseg": f"{catalog}.{schema}.bronze_mseg",
    "silver_mseg": f"{catalog}.{schema}.silver_mseg",
    "silver_stage_events": f"{catalog}.{schema}.silver_stage_events",
    "quarantine_mseg": f"{catalog}.{schema}.quarantine_mseg",
    "gold_stage": f"{catalog}.{schema}.gold_stage_lead_times",
    "gold_d2d": f"{catalog}.{schema}.gold_d2d_lead_times",
    "gold_e2e": f"{catalog}.{schema}.gold_e2e_lead_times",
    "dq_results": f"{catalog}.{schema}.dq_results",
    "dq_run_snapshots": f"{catalog}.{schema}.dq_run_snapshots",
    "dq_agent_reports": f"{catalog}.{schema}.dq_agent_reports",
}
for k, v in TABLES.items():
    print(f"{k:24} -> {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12-stage chain
# MAGIC supplier → procurement → inbound logistics → receipt → quality release → manufacturing → packaging → FG → warehouse → DC → customer shipment → customer
