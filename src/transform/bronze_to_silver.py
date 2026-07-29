from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.quality.runner import load_config, read_csv

EMAIL_PATTERN = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"


def write_csv(df: DataFrame, path: str | Path) -> None:
    """Write a single CSV file without pandas (Python 3.13 friendly)."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    rows = [row.asDict(recursive=True) for row in df.collect()]
    fieldnames = list(df.columns)
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _blank(col: str):
    return F.col(col).isNull() | (F.trim(F.col(col).cast("string")) == "")


def _with_reason(df: DataFrame, reason: str) -> DataFrame:
    return df.withColumn("rejection_reason", F.lit(reason)).withColumn(
        "rejected_at", F.lit(datetime.now(timezone.utc).isoformat())
    )


def _union_quarantine(parts: list[DataFrame], bronze_cols: list[str]) -> DataFrame:
    quarantine = parts[0]
    for part in parts[1:]:
        quarantine = quarantine.unionByName(part, allowMissingColumns=True)
    return quarantine.select(*bronze_cols, "rejection_reason", "rejected_at")


def transform_customers(
    bronze: DataFrame,
    silver_cols: list[str],
) -> tuple[DataFrame, DataFrame]:
    """Clean customers into silver; reject bad/duplicate rows to quarantine."""
    ordered = bronze.withColumn("_row", F.monotonically_increasing_id())
    ranked = ordered.withColumn(
        "_rn", F.row_number().over(Window.partitionBy("customer_id").orderBy("_row"))
    )

    dupes = _with_reason(ranked.filter(F.col("_rn") > 1), "duplicate_customer_id").drop(
        "_row", "_rn"
    )
    unique = ranked.filter(F.col("_rn") == 1).drop("_row", "_rn")

    bad_email = unique.filter(_blank("email") | ~F.col("email").rlike(EMAIL_PATTERN))
    q_email = _with_reason(bad_email, "invalid_or_blank_email")
    ok_email = unique.filter(~_blank("email") & F.col("email").rlike(EMAIL_PATTERN))

    bad_name = ok_email.filter(_blank("name"))
    q_name = _with_reason(bad_name, "blank_name")
    ok_name = ok_email.filter(~_blank("name"))

    silver = ok_name.withColumn(
        "country",
        F.when(_blank("country"), F.lit("UNKNOWN")).otherwise(F.col("country")),
    ).select(*silver_cols)

    quarantine = _union_quarantine([dupes, q_email, q_name], list(bronze.columns))
    return silver, quarantine


def transform_orders(
    bronze: DataFrame,
    silver_customers: DataFrame,
    silver_cols: list[str],
    allowed_status: list[str],
) -> tuple[DataFrame, DataFrame]:
    """Clean orders into silver; reject bad/orphan/duplicate rows to quarantine."""
    ordered = bronze.withColumn("_row", F.monotonically_increasing_id())
    ranked = ordered.withColumn(
        "_rn", F.row_number().over(Window.partitionBy("order_id").orderBy("_row"))
    )

    dupes = _with_reason(ranked.filter(F.col("_rn") > 1), "duplicate_order_id").drop(
        "_row", "_rn"
    )
    unique = ranked.filter(F.col("_rn") == 1).drop("_row", "_rn")

    bad_amount = unique.filter(
        F.col("amount").isNull() | (F.col("amount").cast("double") <= F.lit(0.0))
    )
    q_amount = _with_reason(bad_amount, "non_positive_or_null_amount")
    ok_amount = unique.filter(
        F.col("amount").isNotNull() & (F.col("amount").cast("double") > F.lit(0.0))
    )

    bad_status = ok_amount.filter(
        F.col("status").isNull() | ~F.col("status").isin(allowed_status)
    )
    q_status = _with_reason(bad_status, "invalid_status")
    ok_status = ok_amount.filter(F.col("status").isin(allowed_status))

    bad_fk_null = ok_status.filter(_blank("customer_id"))
    q_fk_null = _with_reason(bad_fk_null, "blank_customer_id")
    ok_fk = ok_status.filter(~_blank("customer_id"))

    valid_customers = silver_customers.select("customer_id").distinct()
    orphans = ok_fk.join(valid_customers, "customer_id", "left_anti")
    q_orphans = _with_reason(orphans, "orphan_customer_id")
    ok_ref = ok_fk.join(valid_customers, "customer_id", "inner")

    silver = ok_ref.select(*silver_cols)
    quarantine = _union_quarantine(
        [dupes, q_amount, q_status, q_fk_null, q_orphans],
        list(bronze.columns),
    )
    return silver, quarantine


def run_bronze_to_silver(spark: SparkSession, config_path: str | Path) -> dict[str, Any]:
    """Run full Bronze -> Silver transform and write quarantine tables."""
    config = load_config(config_path)
    tables = config["tables"]
    summary: dict[str, Any] = {"tables": {}}

    cust_cfg = tables["customers"]
    bronze_customers = read_csv(spark, cust_cfg["bronze_path"])
    silver_customers, q_customers = transform_customers(
        bronze_customers,
        silver_cols=cust_cfg["expected_columns"]["silver"],
    )
    write_csv(silver_customers, cust_cfg["silver_path"])
    write_csv(q_customers, cust_cfg["quarantine_path"])
    summary["tables"]["customers"] = {
        "bronze": bronze_customers.count(),
        "silver": silver_customers.count(),
        "quarantine": q_customers.count(),
        "silver_path": cust_cfg["silver_path"],
        "quarantine_path": cust_cfg["quarantine_path"],
    }

    ord_cfg = tables["orders"]
    bronze_orders = read_csv(spark, ord_cfg["bronze_path"])
    silver_orders, q_orders = transform_orders(
        bronze_orders,
        silver_customers,
        silver_cols=ord_cfg["expected_columns"]["silver"],
        allowed_status=ord_cfg["rules"]["allowed_status"],
    )
    write_csv(silver_orders, ord_cfg["silver_path"])
    write_csv(q_orders, ord_cfg["quarantine_path"])
    summary["tables"]["orders"] = {
        "bronze": bronze_orders.count(),
        "silver": silver_orders.count(),
        "quarantine": q_orders.count(),
        "silver_path": ord_cfg["silver_path"],
        "quarantine_path": ord_cfg["quarantine_path"],
    }
    return summary
