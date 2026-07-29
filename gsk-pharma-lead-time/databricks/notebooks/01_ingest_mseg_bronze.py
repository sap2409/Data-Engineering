# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest Bronze MSEG
# MAGIC Loads synthetic SAP MSEG-style material documents into Delta.
# MAGIC Tries Repo/sample path first; override with widget `bronze_csv` if needed.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "mseg_lead_time")
dbutils.widgets.text("bronze_csv", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
bronze_csv = dbutils.widgets.get("bronze_csv").strip()

try:
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"USE SCHEMA {schema}")
except Exception:
    spark.sql(f"USE {schema}")
    catalog = "hive_metastore"

# COMMAND ----------

from pathlib import Path


def resolve_default_csv() -> str:
    """Locate sample CSV relative to this notebook in a Databricks Repo."""
    nb = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    # .../gsk-pharma-lead-time/databricks/notebooks/01_...
    parts = nb.strip("/").split("/")
    # climb to gsk-pharma-lead-time
    if "databricks" in parts:
        idx = parts.index("databricks")
        root_parts = parts[:idx]
    else:
        root_parts = parts[:-2]
    root = "/Workspace/" + "/".join(root_parts)
    candidates = [
        f"{root}/databricks/sample_data/mseg_material_documents.csv",
        f"{root}/data/bronze/mseg_material_documents.csv",
    ]
    for c in candidates:
        if Path(c).exists():
            return c
    return candidates[0]


path = bronze_csv or resolve_default_csv()
print("Reading MSEG CSV from:", path)

bronze = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("nullValue", "")
    .csv(path)
)

bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.bronze_mseg"
)
print("bronze_mseg rows:", bronze.count())
display(bronze.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC **Talking point:** Bronze is raw SAP MSEG-style goods movements (`BWART`, `CHARG`, `MATNR`, timestamps).
