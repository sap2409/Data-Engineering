# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold lead times (Stage / D2D / E2E)
# MAGIC Builds KPI tables from Silver stage events.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "mseg_lead_time")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
try:
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"USE SCHEMA {schema}")
except Exception:
    spark.sql(f"USE {schema}")
    catalog = "hive_metastore"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

STAGES = [
    "supplier",
    "raw_material_procurement",
    "inbound_logistics",
    "raw_material_receipt",
    "quality_inspection_and_release",
    "manufacturing",
    "packaging",
    "finished_goods",
    "warehouse",
    "distribution_center",
    "customer_shipment",
    "customer",
]

stages_df = spark.table(f"{catalog}.{schema}.silver_stage_events")

stage_lt = (
    stages_df.filter(F.col("end_ts").isNotNull() & F.col("start_ts").isNotNull())
    .withColumn(
        "stage_lead_time_days",
        F.round((F.col("end_ts").cast("long") - F.col("start_ts").cast("long")) / 86400.0, 2),
    )
    .select(
        "batch_id",
        "material",
        "plant",
        "stage",
        "stage_order",
        "start_ts",
        "end_ts",
        "stage_lead_time_days",
        "mseg_line_count",
    )
)

w = Window.partitionBy("batch_id").orderBy("stage_order")
d2d_lt = (
    stages_df.filter(F.col("end_ts").isNotNull() & F.col("start_ts").isNotNull())
    .withColumn("next_stage", F.lead("stage").over(w))
    .withColumn("next_start_ts", F.lead("start_ts").over(w))
    .withColumn("next_stage_order", F.lead("stage_order").over(w))
    .filter(F.col("next_stage").isNotNull())
    .filter(F.col("next_stage_order") == F.col("stage_order") + 1)
    .withColumn(
        "d2d_lead_time_days",
        F.round(
            (F.col("next_start_ts").cast("long") - F.col("end_ts").cast("long")) / 86400.0,
            2,
        ),
    )
    .withColumn(
        "d2d_lead_time_days",
        F.when(F.col("d2d_lead_time_days") < 0, F.lit(0.0)).otherwise(F.col("d2d_lead_time_days")),
    )
    .select(
        "batch_id",
        "material",
        "plant",
        F.col("stage").alias("from_stage"),
        F.col("next_stage").alias("to_stage"),
        F.col("end_ts").alias("from_end_ts"),
        F.col("next_start_ts").alias("to_start_ts"),
        "d2d_lead_time_days",
    )
)

first, last = STAGES[0], STAGES[-1]
starts = stages_df.filter(F.col("stage") == first).select(
    "batch_id",
    "material",
    "plant",
    F.col("start_ts").alias("lifecycle_start"),
)
ends = stages_df.filter(F.col("stage") == last).select(
    "batch_id", F.col("end_ts").alias("lifecycle_end")
)
stage_counts = stages_df.groupBy("batch_id").agg(F.countDistinct("stage").alias("stages_present"))
e2e_lt = (
    starts.join(ends, "batch_id", "inner")
    .join(stage_counts, "batch_id", "left")
    .withColumn(
        "e2e_lead_time_days",
        F.round(
            (F.col("lifecycle_end").cast("long") - F.col("lifecycle_start").cast("long"))
            / 86400.0,
            2,
        ),
    )
)

stage_lt.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.gold_stage_lead_times"
)
d2d_lt.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.gold_d2d_lead_times"
)
e2e_lt.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.gold_e2e_lead_times"
)

print("stage rows:", stage_lt.count(), "| d2d:", d2d_lt.count(), "| e2e:", e2e_lt.count())
display(e2e_lt)
display(d2d_lt.orderBy(F.desc("d2d_lead_time_days")).limit(20))
