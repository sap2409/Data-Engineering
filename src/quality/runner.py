from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession

from src.quality.checks import (
    CheckResult,
    check_allowed_values,
    check_amount_positive,
    check_email_format,
    check_nulls,
    check_quarantine_reconciliation,
    check_referential_integrity,
    check_row_counts,
    check_schema,
    check_uniqueness,
)


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


def run_table_checks(
    spark: SparkSession,
    table_name: str,
    table_cfg: dict[str, Any],
    thresholds: dict[str, Any],
) -> dict[str, Any]:
    bronze = read_csv(spark, table_cfg["bronze_path"])
    silver = read_csv(spark, table_cfg["silver_path"])
    rules = table_cfg.get("rules", {})
    expected = table_cfg.get("expected_columns", {})
    results: list[CheckResult] = []

    quarantine_path = table_cfg.get("quarantine_path")
    if quarantine_path and Path(quarantine_path).exists():
        quarantine = read_csv(spark, quarantine_path)
        results.append(check_quarantine_reconciliation(bronze, silver, quarantine))
    else:
        results.append(
            check_row_counts(
                bronze,
                silver,
                max_drop_pct=float(rules.get("max_row_drop_pct", 10)),
                warn_pct=float(thresholds.get("warn_row_drop_pct", 5)),
                fail_pct=float(thresholds.get("fail_row_drop_pct", 15)),
            )
        )

    results.append(
        check_schema(
            bronze,
            silver,
            expected_bronze=expected.get("bronze"),
            expected_silver=expected.get("silver"),
        )
    )

    # Silver is the contract layer — enforce quality there.
    results.extend(check_nulls(silver, rules.get("max_null_pct", {}), layer="silver"))
    results.extend(check_uniqueness(silver, rules.get("unique_columns", []), layer="silver"))

    if rules.get("email_format"):
        results.append(check_email_format(silver, layer="silver"))

    if rules.get("amount_positive"):
        results.append(check_amount_positive(silver, layer="silver"))

    if "allowed_status" in rules:
        results.append(
            check_allowed_values(silver, "status", rules["allowed_status"], layer="silver")
        )

    return {
        "table": table_name,
        "primary_key": table_cfg.get("primary_key"),
        "overall_status": _overall_status(results),
        "checks": [r.to_dict() for r in results],
        "bronze_path": table_cfg["bronze_path"],
        "silver_path": table_cfg["silver_path"],
        "quarantine_path": quarantine_path,
    }


def run_cross_table_checks(spark: SparkSession, config: dict[str, Any]) -> dict[str, Any]:
    tables = config["tables"]
    if "orders" not in tables or "customers" not in tables:
        return {
            "table": "_cross_",
            "overall_status": "PASS",
            "checks": [],
            "message": "Skipped referential checks (orders/customers not both configured)",
        }

    silver_orders = read_csv(spark, tables["orders"]["silver_path"])
    silver_customers = read_csv(spark, tables["customers"]["silver_path"])

    results = [
        check_referential_integrity(silver_orders, silver_customers, layer="silver"),
    ]
    return {
        "table": "_cross_",
        "overall_status": _overall_status(results),
        "checks": [r.to_dict() for r in results],
    }


def run_all_checks(spark: SparkSession, config_path: str | Path) -> dict[str, Any]:
    config = load_config(config_path)
    thresholds = config.get("thresholds", {})
    table_reports = [
        run_table_checks(spark, name, cfg, thresholds)
        for name, cfg in config["tables"].items()
    ]
    cross = run_cross_table_checks(spark, config)
    all_reports = table_reports + [cross]
    return {
        "overall_status": _overall_status_from_reports(all_reports),
        "tables": all_reports,
    }


def _overall_status(results: list[CheckResult]) -> str:
    statuses = {r.status for r in results}
    if "FAIL" in statuses:
        return "FAIL"
    if "WARN" in statuses:
        return "WARN"
    return "PASS"


def _overall_status_from_reports(reports: list[dict[str, Any]]) -> str:
    statuses = {r["overall_status"] for r in reports}
    if "FAIL" in statuses:
        return "FAIL"
    if "WARN" in statuses:
        return "WARN"
    return "PASS"
