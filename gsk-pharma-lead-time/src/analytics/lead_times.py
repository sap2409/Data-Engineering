from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.transform.bronze_to_silver import load_config, read_csv, write_csv


def build_stage_lead_times(stages_df: DataFrame) -> DataFrame:
    return (
        stages_df.filter(F.col("end_ts").isNotNull() & F.col("start_ts").isNotNull())
        .withColumn(
            "stage_lead_time_days",
            F.round(
                (F.col("end_ts").cast("long") - F.col("start_ts").cast("long")) / 86400.0,
                2,
            ),
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


def build_d2d_lead_times(stages_df: DataFrame) -> DataFrame:
    """D2D = handoff wait from end(stage N) to start(stage N+1) in configured order."""
    base = stages_df.filter(F.col("end_ts").isNotNull() & F.col("start_ts").isNotNull())
    w = Window.partitionBy("batch_id").orderBy("stage_order")
    return (
        base.withColumn("next_stage", F.lead("stage").over(w))
        .withColumn("next_start_ts", F.lead("start_ts").over(w))
        .withColumn("next_stage_order", F.lead("stage_order").over(w))
        .filter(F.col("next_stage").isNotNull())
        .filter(F.col("next_stage_order") == F.col("stage_order") + 1)
        .withColumn(
            "d2d_lead_time_days",
            F.round(
                (F.col("next_start_ts").cast("long") - F.col("end_ts").cast("long"))
                / 86400.0,
                2,
            ),
        )
        # Overlapping stages (next starts before prior ends) => 0 handoff wait
        .withColumn(
            "d2d_lead_time_days",
            F.when(F.col("d2d_lead_time_days") < 0, F.lit(0.0)).otherwise(
                F.col("d2d_lead_time_days")
            ),
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


def build_e2e_lead_times(stages_df: DataFrame, stages: list[str]) -> DataFrame:
    """E2E = supplier start → customer end when both endpoints exist."""
    first, last = stages[0], stages[-1]
    starts = (
        stages_df.filter(F.col("stage") == first)
        .select(
            "batch_id",
            F.col("material").alias("material"),
            F.col("plant").alias("plant"),
            F.col("start_ts").alias("lifecycle_start"),
        )
    )
    ends = (
        stages_df.filter(F.col("stage") == last)
        .select("batch_id", F.col("end_ts").alias("lifecycle_end"))
    )
    stage_counts = stages_df.groupBy("batch_id").agg(
        F.countDistinct("stage").alias("stages_present")
    )
    return (
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
        .select(
            "batch_id",
            "material",
            "plant",
            "lifecycle_start",
            "lifecycle_end",
            "e2e_lead_time_days",
            "stages_present",
        )
    )


def run_lead_time_analytics(spark: SparkSession, config_path: str | Path) -> dict[str, Any]:
    config = load_config(config_path)
    paths = config["paths"]
    stages = config["stages"]
    stage_events = (
        read_csv(spark, paths["silver_stages"])
        .withColumn("start_ts", F.to_timestamp("start_ts"))
        .withColumn("end_ts", F.to_timestamp("end_ts"))
    )

    stage_lt = build_stage_lead_times(stage_events)
    d2d_lt = build_d2d_lead_times(stage_events)
    e2e_lt = build_e2e_lead_times(stage_events, stages)

    write_csv(stage_lt, paths["gold_stage"])
    write_csv(d2d_lt, paths["gold_d2d"])
    write_csv(e2e_lt, paths["gold_e2e"])

    return {
        "stage_rows": stage_lt.count(),
        "d2d_rows": d2d_lt.count(),
        "e2e_rows": e2e_lt.count(),
        "paths": {
            "stage": paths["gold_stage"],
            "d2d": paths["gold_d2d"],
            "e2e": paths["gold_e2e"],
        },
    }
