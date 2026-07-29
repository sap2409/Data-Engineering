from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import functions as F

from src.transform.bronze_to_silver import load_config, read_csv


def _result(name: str, status: str, message: str, **metrics: Any) -> dict[str, Any]:
    return {"name": name, "status": status, "message": message, "metrics": metrics}


def run_quality_checks(spark, config_path: str | Path) -> dict[str, Any]:
    config = load_config(config_path)
    paths = config["paths"]
    rules = config["rules"]
    targets = config.get("targets", {})
    stages = config["stages"]

    bronze = read_csv(spark, paths["bronze"])
    silver_mseg = read_csv(spark, paths["silver_mseg"])
    quarantine = read_csv(spark, paths["quarantine"])
    stage_events = read_csv(spark, paths["silver_stages"])
    stage_lt = read_csv(spark, paths["gold_stage"])
    d2d_lt = read_csv(spark, paths["gold_d2d"])
    e2e_lt = read_csv(spark, paths["gold_e2e"])

    checks: list[dict[str, Any]] = []
    b, s, q = bronze.count(), silver_mseg.count(), quarantine.count()
    checks.append(
        _result(
            "mseg_quarantine_reconciliation",
            "PASS" if b == s + q else "FAIL",
            f"Bronze MSEG={b}, Silver MSEG={s}, Quarantine={q}",
            bronze=b,
            silver=s,
            quarantine=q,
        )
    )

    for col, limit in rules.get("max_null_pct_stages", {}).items():
        total = stage_events.count() or 1
        nulls = stage_events.filter(
            F.col(col).isNull() | (F.trim(F.col(col).cast("string")) == "")
        ).count()
        pct = nulls / total * 100
        checks.append(
            _result(
                f"null_pct_stage_{col}",
                "PASS" if pct <= limit else "FAIL",
                f"stage_events.{col}: {nulls}/{total} ({pct:.1f}%)",
            )
        )

    bad_stage = stage_events.filter(~F.col("stage").isin(stages)).count()
    checks.append(
        _result(
            "allowed_stages",
            "PASS" if bad_stage == 0 else "FAIL",
            f"invalid stage rows: {bad_stage}",
        )
    )

    total_se = stage_events.count()
    distinct_se = stage_events.select("batch_id", "stage").distinct().count()
    checks.append(
        _result(
            "unique_batch_stage",
            "PASS" if total_se == distinct_se else "FAIL",
            f"duplicate batch+stage: {total_se - distinct_se}",
        )
    )

    neg = (
        stage_lt.filter(F.col("stage_lead_time_days") < 0).count()
        + d2d_lt.filter(F.col("d2d_lead_time_days") < 0).count()
        + e2e_lt.filter(F.col("e2e_lead_time_days") < 0).count()
    )
    checks.append(
        _result(
            "non_negative_lead_times",
            "PASS" if neg == 0 else "FAIL",
            f"negative lead-time rows: {neg}",
        )
    )

    e2e_target = float(targets.get("e2e_days", 60))
    over_e2e = e2e_lt.filter(F.col("e2e_lead_time_days") > e2e_target).count()
    checks.append(
        _result(
            "e2e_target_breach",
            "WARN" if over_e2e else "PASS",
            f"{over_e2e} batch(es) above E2E target {e2e_target}d",
            over_target=over_e2e,
        )
    )

    d2d_target = float(targets.get("d2d_days", 2))
    over_d2d = d2d_lt.filter(F.col("d2d_lead_time_days") > d2d_target).count()
    checks.append(
        _result(
            "d2d_target_breach",
            "WARN" if over_d2d else "PASS",
            f"{over_d2d} handoff(s) above D2D target {d2d_target}d",
            over_target=over_d2d,
        )
    )

    statuses = {c["status"] for c in checks}
    overall = "FAIL" if "FAIL" in statuses else ("WARN" if "WARN" in statuses else "PASS")

    summary = {
        "avg_e2e_days": _avg(e2e_lt, "e2e_lead_time_days"),
        "avg_d2d_days": _avg(d2d_lt, "d2d_lead_time_days"),
        "avg_stage_days_by_stage": {
            r["stage"]: r["avg_days"]
            for r in stage_lt.groupBy("stage")
            .agg(F.round(F.avg("stage_lead_time_days"), 2).alias("avg_days"))
            .collect()
        },
        "batches_with_e2e": e2e_lt.count(),
        "d2d_handoffs": d2d_lt.count(),
        "stage_count_expected": len(stages),
    }
    return {"overall_status": overall, "checks": checks, "kpi_summary": summary}


def _avg(df, col: str):
    if df.count() == 0:
        return None
    val = df.agg(F.round(F.avg(col), 2).alias("v")).collect()[0]["v"]
    return float(val) if val is not None else None
