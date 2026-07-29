# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Transform Silver (clean MSEG + stage events + quarantine)
# MAGIC Maps `BWART` → 12 lifecycle stages; quarantines invalid postings.

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

from datetime import datetime, timezone

from pyspark.sql import DataFrame
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

BWART_STAGE_MAP = {
    "101": "raw_material_receipt",
    "103": "raw_material_receipt",
    "105": "quality_inspection_and_release",
    "321": "quality_inspection_and_release",
    "322": "quality_inspection_and_release",
    "261": "manufacturing",
    "262": "manufacturing",
    "101F": "manufacturing",
    "531": "manufacturing",
    "311": "warehouse",
    "313": "warehouse",
    "315": "warehouse",
    "601": "customer_shipment",
    "602": "customer_shipment",
    "641": "distribution_center",
    "643": "distribution_center",
    "647": "distribution_center",
    "Z07": "distribution_center",
    "101S": "supplier",
    "541": "raw_material_procurement",
    "542": "raw_material_procurement",
    "561": "inbound_logistics",
    "562": "inbound_logistics",
    "309": "packaging",
    "501": "finished_goods",
    "Z01": "supplier",
    "Z02": "raw_material_procurement",
    "Z03": "inbound_logistics",
    "Z04": "packaging",
    "Z05": "finished_goods",
    "Z06": "customer",
}


def _blank(col: str):
    return F.col(col).isNull() | (F.trim(F.col(col).cast("string")) == "")


def _with_reason(df: DataFrame, reason: str) -> DataFrame:
    return df.withColumn("rejection_reason", F.lit(reason)).withColumn(
        "rejected_at", F.lit(datetime.now(timezone.utc).isoformat())
    )


def _bwart_map_expr():
    mapping = F.create_map(*[F.lit(x) for kv in BWART_STAGE_MAP.items() for x in kv])
    return mapping[F.col("BWART").cast("string")]


def clean_mseg(bronze: DataFrame):
    ordered = bronze.withColumn("_row", F.monotonically_increasing_id())
    ranked = ordered.withColumn(
        "_rn",
        F.row_number().over(Window.partitionBy("MBLNR", "MJAHR", "ZEILE").orderBy("_row")),
    )
    dupes = _with_reason(ranked.filter(F.col("_rn") > 1), "duplicate_mseg_key").drop("_row", "_rn")
    unique = ranked.filter(F.col("_rn") == 1).drop("_row", "_rn")

    bad_mat = unique.filter(_blank("MATNR") | _blank("CHARG") | _blank("BWART") | _blank("WERKS"))
    q_mat = _with_reason(bad_mat, "missing_matnr_charg_bwart_or_werks")
    ok_mat = unique.filter(~_blank("MATNR") & ~_blank("CHARG") & ~_blank("BWART") & ~_blank("WERKS"))

    with_stage = ok_mat.withColumn("stage", _bwart_map_expr())
    bad_bwart = with_stage.filter(F.col("stage").isNull())
    q_bwart = _with_reason(bad_bwart.drop("stage"), "unmapped_bwart")
    ok_bwart = with_stage.filter(F.col("stage").isNotNull())

    bad_qty = ok_bwart.filter(F.col("MENGE").isNull() | (F.col("MENGE").cast("double") < 0))
    q_qty = _with_reason(bad_qty.drop("stage"), "invalid_menge")
    ok_qty = ok_bwart.filter(F.col("MENGE").isNotNull() & (F.col("MENGE").cast("double") >= 0))

    parsed = ok_qty.withColumn(
        "event_ts", F.coalesce(F.to_timestamp("CPUDT_TIME"), F.to_timestamp("BUDAT"))
    )
    bad_ts = parsed.filter(F.col("event_ts").isNull())
    q_ts = _with_reason(bad_ts.drop("stage", "event_ts"), "invalid_timestamp")
    silver_mseg = parsed.filter(F.col("event_ts").isNotNull())

    quarantine = dupes
    for part in [q_mat, q_bwart, q_qty, q_ts]:
        quarantine = quarantine.unionByName(part, allowMissingColumns=True)
    quarantine = quarantine.select(*[c for c in bronze.columns], "rejection_reason", "rejected_at")
    return silver_mseg, quarantine


def mseg_to_stage_events(silver_mseg: DataFrame):
    agg = silver_mseg.groupBy(F.col("CHARG").alias("batch_id"), "stage").agg(
        F.first("WERKS", ignorenulls=True).alias("plant"),
        F.first("MATNR", ignorenulls=True).alias("material"),
        F.min("event_ts").alias("start_ts"),
        F.max("event_ts").alias("end_ts"),
        F.count(F.lit(1)).alias("mseg_line_count"),
        F.sum(F.col("MENGE").cast("double")).alias("total_menge"),
        F.first("MEINS", ignorenulls=True).alias("uom"),
        F.first("LIFNR", ignorenulls=True).alias("vendor"),
        F.first("KUNNR", ignorenulls=True).alias("customer"),
    )
    order_map = {s: i + 1 for i, s in enumerate(STAGES)}
    mapping = F.create_map(*[F.lit(x) for kv in order_map.items() for x in kv])
    return agg.withColumn("stage_order", mapping[F.col("stage")])

# COMMAND ----------

bronze = spark.table(f"{catalog}.{schema}.bronze_mseg")
silver_mseg, quarantine = clean_mseg(bronze)
stage_events = mseg_to_stage_events(silver_mseg)

silver_mseg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.silver_mseg"
)
stage_events.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.silver_stage_events"
)
quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.quarantine_mseg"
)

display(
    spark.createDataFrame(
        [
            (
                bronze.count(),
                silver_mseg.count(),
                stage_events.count(),
                quarantine.count(),
            )
        ],
        ["bronze_mseg", "silver_mseg", "stage_events", "quarantine"],
    )
)

# COMMAND ----------

display(
    spark.table(f"{catalog}.{schema}.quarantine_mseg")
    .groupBy("rejection_reason")
    .count()
    .orderBy(F.desc("count"))
)
