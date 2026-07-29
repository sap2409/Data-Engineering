from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def load_config(path: str | Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def read_csv(spark: SparkSession, path: str | Path) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("nullValue", "")
        .csv(str(path))
    )


def write_csv(df: DataFrame, path: str | Path) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    rows = [row.asDict(recursive=True) for row in df.collect()]
    if not rows:
        # still write header
        with target.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(df.columns))
            writer.writeheader()
        return
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(df.columns))
        writer.writeheader()
        writer.writerows(rows)


def _blank(col: str):
    return F.col(col).isNull() | (F.trim(F.col(col).cast("string")) == "")


def _with_reason(df: DataFrame, reason: str) -> DataFrame:
    return df.withColumn("rejection_reason", F.lit(reason)).withColumn(
        "rejected_at", F.lit(datetime.now(timezone.utc).isoformat())
    )


def _bwart_map_expr(bwart_stage_map: dict[str, str]):
    # Cast BWART to string for lookup; keys may be numeric in CSV inference
    mapping = F.create_map(*[F.lit(x) for kv in bwart_stage_map.items() for x in kv])
    return mapping[F.col("BWART").cast("string")]


def clean_mseg(bronze: DataFrame, config: dict[str, Any]) -> tuple[DataFrame, DataFrame]:
    """Validate MSEG lines; quarantine bad keys / qty / unknown BWART."""
    bwart_map = config["bwart_stage_map"]

    ordered = bronze.withColumn("_row", F.monotonically_increasing_id())
    ranked = ordered.withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("MBLNR", "MJAHR", "ZEILE").orderBy("_row")
        ),
    )
    dupes = _with_reason(ranked.filter(F.col("_rn") > 1), "duplicate_mseg_key").drop(
        "_row", "_rn"
    )
    unique = ranked.filter(F.col("_rn") == 1).drop("_row", "_rn")

    bad_mat = unique.filter(_blank("MATNR") | _blank("CHARG") | _blank("BWART") | _blank("WERKS"))
    q_mat = _with_reason(bad_mat, "missing_matnr_charg_bwart_or_werks")
    ok_mat = unique.filter(
        ~_blank("MATNR") & ~_blank("CHARG") & ~_blank("BWART") & ~_blank("WERKS")
    )

    with_stage = ok_mat.withColumn("stage", _bwart_map_expr(bwart_map))
    bad_bwart = with_stage.filter(F.col("stage").isNull())
    q_bwart = _with_reason(bad_bwart.drop("stage"), "unmapped_bwart")
    ok_bwart = with_stage.filter(F.col("stage").isNotNull())

    bad_qty = ok_bwart.filter(F.col("MENGE").isNull() | (F.col("MENGE").cast("double") < 0))
    q_qty = _with_reason(bad_qty.drop("stage"), "invalid_menge")
    ok_qty = ok_bwart.filter(F.col("MENGE").isNotNull() & (F.col("MENGE").cast("double") >= 0))

    parsed = ok_qty.withColumn(
        "event_ts",
        F.coalesce(F.to_timestamp("CPUDT_TIME"), F.to_timestamp("BUDAT")),
    )
    bad_ts = parsed.filter(F.col("event_ts").isNull())
    q_ts = _with_reason(bad_ts.drop("stage", "event_ts"), "invalid_timestamp")
    silver_mseg = parsed.filter(F.col("event_ts").isNotNull())

    parts = [dupes, q_mat, q_bwart, q_qty, q_ts]
    quarantine = parts[0]
    for part in parts[1:]:
        quarantine = quarantine.unionByName(part, allowMissingColumns=True)
    quarantine = quarantine.select(
        *[c for c in bronze.columns],
        "rejection_reason",
        "rejected_at",
    )
    return silver_mseg, quarantine


def mseg_to_stage_events(silver_mseg: DataFrame, stages: list[str]) -> DataFrame:
    """Collapse MSEG lines to one row per batch (CHARG) + stage with start/end."""
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
    order_map = {s: i + 1 for i, s in enumerate(stages)}
    mapping = F.create_map(*[F.lit(x) for kv in order_map.items() for x in kv])
    return agg.withColumn("stage_order", mapping[F.col("stage")]).orderBy(
        "batch_id", "stage_order"
    )


def run_bronze_to_silver(spark: SparkSession, config_path: str | Path) -> dict[str, Any]:
    config = load_config(config_path)
    paths = config["paths"]
    bronze = read_csv(spark, paths["bronze"])
    silver_mseg, quarantine = clean_mseg(bronze, config)
    stage_events = mseg_to_stage_events(silver_mseg, config["stages"])

    # Persist cleaned MSEG without internal helper noise beyond stage/event_ts
    write_csv(silver_mseg, paths["silver_mseg"])
    write_csv(stage_events, paths["silver_stages"])
    write_csv(quarantine, paths["quarantine"])

    return {
        "bronze": bronze.count(),
        "silver_mseg": silver_mseg.count(),
        "silver_stages": stage_events.count(),
        "quarantine": quarantine.count(),
        "paths": paths,
    }
