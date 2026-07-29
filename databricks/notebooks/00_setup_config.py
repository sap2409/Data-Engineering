# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup config
# MAGIC Creates catalog/schema widgets and shared table names for the Bronze→Silver DQ demo.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "ecommerce_dq")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# Unity Catalog workspaces use catalogs; older/Hive-only use databases.
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"USE SCHEMA {schema}")
    print(f"Using Unity Catalog: {catalog}.{schema}")
except Exception:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    spark.sql(f"USE {schema}")
    catalog = "hive_metastore"
    print(f"Using Hive metastore database: {schema}")

# COMMAND ----------

# Shared naming — other notebooks re-declare widgets the same way
TABLES = {
    "bronze_customers": f"{catalog}.{schema}.bronze_customers",
    "bronze_orders": f"{catalog}.{schema}.bronze_orders",
    "silver_customers": f"{catalog}.{schema}.silver_customers",
    "silver_orders": f"{catalog}.{schema}.silver_orders",
    "quarantine_customers": f"{catalog}.{schema}.quarantine_customers",
    "quarantine_orders": f"{catalog}.{schema}.quarantine_orders",
    "dq_results": f"{catalog}.{schema}.dq_results",
    "dq_agent_reports": f"{catalog}.{schema}.dq_agent_reports",
}

for name, fqn in TABLES.items():
    print(f"{name:24} -> {fqn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interview note
# MAGIC - **Bronze** = raw as-landed  
# MAGIC - **Silver** = cleaned contract  
# MAGIC - **Quarantine** = rejected rows + `rejection_reason`  
# MAGIC - **dq_results** = quality gate history
