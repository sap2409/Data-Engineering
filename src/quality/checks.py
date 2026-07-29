from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class CheckResult:
    name: str
    status: str  # PASS | WARN | FAIL
    message: str
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _status_from_drop(drop_pct: float, warn_pct: float, fail_pct: float) -> str:
    if drop_pct >= fail_pct:
        return "FAIL"
    if drop_pct >= warn_pct:
        return "WARN"
    return "PASS"


def check_row_counts(
    bronze: DataFrame,
    silver: DataFrame,
    *,
    max_drop_pct: float,
    warn_pct: float,
    fail_pct: float,
) -> CheckResult:
    bronze_count = bronze.count()
    silver_count = silver.count()
    dropped = max(bronze_count - silver_count, 0)
    drop_pct = (dropped / bronze_count * 100) if bronze_count else 0.0

    status = _status_from_drop(drop_pct, warn_pct, fail_pct)
    if drop_pct > max_drop_pct:
        status = "FAIL"

    return CheckResult(
        name="row_count_delta",
        status=status,
        message=(
            f"Bronze={bronze_count}, Silver={silver_count}, "
            f"dropped={dropped} ({drop_pct:.1f}%)"
        ),
        metrics={
            "bronze_count": bronze_count,
            "silver_count": silver_count,
            "dropped": dropped,
            "drop_pct": round(drop_pct, 2),
            "max_drop_pct": max_drop_pct,
        },
    )


def check_schema(
    bronze: DataFrame,
    silver: DataFrame,
    expected_bronze: list[str] | None = None,
    expected_silver: list[str] | None = None,
) -> CheckResult:
    bronze_cols = set(bronze.columns)
    silver_cols = set(silver.columns)
    dropped_cols = sorted(bronze_cols - silver_cols)
    added_cols = sorted(silver_cols - bronze_cols)

    expected_dropped: set[str] = set()
    if expected_bronze is not None and expected_silver is not None:
        expected_dropped = set(expected_bronze) - set(expected_silver)

    issues: list[str] = []
    if expected_bronze is not None:
        missing_b = sorted(set(expected_bronze) - bronze_cols)
        extra_b = sorted(bronze_cols - set(expected_bronze))
        if missing_b:
            issues.append(f"bronze missing expected columns: {missing_b}")
        if extra_b:
            issues.append(f"bronze unexpected columns: {extra_b}")
    if expected_silver is not None:
        missing_s = sorted(set(expected_silver) - silver_cols)
        extra_s = sorted(silver_cols - set(expected_silver))
        if missing_s:
            issues.append(f"silver missing expected columns: {missing_s}")
        if extra_s:
            issues.append(f"silver unexpected columns: {extra_s}")

    unexpected_drops = sorted(set(dropped_cols) - expected_dropped)
    if unexpected_drops:
        issues.append(f"unexpected columns dropped: {unexpected_drops}")

    status = "FAIL" if issues else "PASS"

    message_parts = []
    if dropped_cols:
        message_parts.append(f"columns dropped bronze→silver: {dropped_cols}")
    if added_cols:
        message_parts.append(f"columns added in silver: {added_cols}")
    message_parts.extend(issues)

    return CheckResult(
        name="schema_drift",
        status=status,
        message="; ".join(message_parts) if message_parts else "Schemas align with expectations",
        metrics={
            "bronze_columns": sorted(bronze_cols),
            "silver_columns": sorted(silver_cols),
            "dropped_columns": dropped_cols,
            "added_columns": added_cols,
            "expected_dropped": sorted(expected_dropped),
        },
    )


def check_quarantine_reconciliation(
    bronze: DataFrame,
    silver: DataFrame,
    quarantine: DataFrame,
) -> CheckResult:
    """Ensure every Bronze row is accounted for in Silver or quarantine."""
    bronze_count = bronze.count()
    silver_count = silver.count()
    quarantine_count = quarantine.count()
    accounted = silver_count + quarantine_count
    ok = accounted == bronze_count

    missing_reason = 0
    if "rejection_reason" in quarantine.columns:
        missing_reason = quarantine.filter(
            F.col("rejection_reason").isNull()
            | (F.trim(F.col("rejection_reason")) == "")
        ).count()
    else:
        missing_reason = quarantine_count

    status = "PASS" if ok and missing_reason == 0 else "FAIL"
    return CheckResult(
        name="quarantine_reconciliation",
        status=status,
        message=(
            f"Bronze={bronze_count}, Silver={silver_count}, Quarantine={quarantine_count}, "
            f"accounted={accounted}, blank_reasons={missing_reason}"
        ),
        metrics={
            "bronze_count": bronze_count,
            "silver_count": silver_count,
            "quarantine_count": quarantine_count,
            "accounted": accounted,
            "blank_rejection_reasons": missing_reason,
        },
    )


def check_nulls(df: DataFrame, max_null_pct: dict[str, float], layer: str) -> list[CheckResult]:
    total = df.count() or 1
    results: list[CheckResult] = []
    for col, limit in max_null_pct.items():
        if col not in df.columns:
            results.append(
                CheckResult(
                    name=f"null_pct_{layer}_{col}",
                    status="FAIL",
                    message=f"Column '{col}' not found in {layer}",
                    metrics={"column": col, "layer": layer},
                )
            )
            continue
        nulls = df.filter(F.col(col).isNull() | (F.trim(F.col(col).cast("string")) == "")).count()
        pct = nulls / total * 100
        status = "PASS" if pct <= limit else "FAIL"
        results.append(
            CheckResult(
                name=f"null_pct_{layer}_{col}",
                status=status,
                message=f"{layer}.{col}: {nulls}/{total} null/blank ({pct:.1f}%), limit={limit}%",
                metrics={
                    "layer": layer,
                    "column": col,
                    "nulls": nulls,
                    "total": total,
                    "null_pct": round(pct, 2),
                    "limit_pct": limit,
                },
            )
        )
    return results


def check_uniqueness(df: DataFrame, columns: list[str], layer: str) -> list[CheckResult]:
    results: list[CheckResult] = []
    total = df.count()
    for col in columns:
        if col not in df.columns:
            results.append(
                CheckResult(
                    name=f"unique_{layer}_{col}",
                    status="FAIL",
                    message=f"Column '{col}' not found in {layer}",
                    metrics={"column": col, "layer": layer},
                )
            )
            continue
        distinct = df.select(col).distinct().count()
        dupes = total - distinct
        status = "PASS" if dupes == 0 else "FAIL"
        results.append(
            CheckResult(
                name=f"unique_{layer}_{col}",
                status=status,
                message=f"{layer}.{col}: {dupes} duplicate value(s)",
                metrics={
                    "layer": layer,
                    "column": col,
                    "total": total,
                    "distinct": distinct,
                    "duplicates": dupes,
                },
            )
        )
    return results


def check_email_format(df: DataFrame, column: str = "email", layer: str = "silver") -> CheckResult:
    if column not in df.columns:
        return CheckResult(
            name=f"email_format_{layer}",
            status="FAIL",
            message=f"Column '{column}' not found in {layer}",
        )
    pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    bad = df.filter(~F.col(column).rlike(pattern) | F.col(column).isNull()).count()
    status = "PASS" if bad == 0 else "FAIL"
    return CheckResult(
        name=f"email_format_{layer}",
        status=status,
        message=f"{layer}: {bad} invalid email(s)",
        metrics={"invalid_emails": bad, "layer": layer},
    )


def check_amount_positive(df: DataFrame, column: str = "amount", layer: str = "silver") -> CheckResult:
    if column not in df.columns:
        return CheckResult(
            name=f"amount_positive_{layer}",
            status="FAIL",
            message=f"Column '{column}' not found in {layer}",
        )
    bad = df.filter(F.col(column).isNull() | (F.col(column).cast("double") <= 0)).count()
    status = "PASS" if bad == 0 else "FAIL"
    return CheckResult(
        name=f"amount_positive_{layer}",
        status=status,
        message=f"{layer}: {bad} non-positive/null amount(s)",
        metrics={"bad_amounts": bad, "layer": layer},
    )


def check_allowed_values(
    df: DataFrame,
    column: str,
    allowed: list[str],
    layer: str = "silver",
) -> CheckResult:
    if column not in df.columns:
        return CheckResult(
            name=f"allowed_values_{layer}_{column}",
            status="FAIL",
            message=f"Column '{column}' not found in {layer}",
        )
    bad = df.filter(~F.col(column).isin(allowed) | F.col(column).isNull()).count()
    status = "PASS" if bad == 0 else "FAIL"
    return CheckResult(
        name=f"allowed_values_{layer}_{column}",
        status=status,
        message=f"{layer}.{column}: {bad} value(s) outside {allowed}",
        metrics={"bad_values": bad, "allowed": allowed, "layer": layer, "column": column},
    )


def check_referential_integrity(
    orders: DataFrame,
    customers: DataFrame,
    order_fk: str = "customer_id",
    customer_pk: str = "customer_id",
    layer: str = "silver",
) -> CheckResult:
    orphans = (
        orders.alias("o")
        .join(customers.alias("c"), F.col(f"o.{order_fk}") == F.col(f"c.{customer_pk}"), "left_anti")
        .count()
    )
    status = "PASS" if orphans == 0 else "FAIL"
    return CheckResult(
        name=f"referential_integrity_{layer}",
        status=status,
        message=f"{layer}: {orphans} order(s) with missing customer",
        metrics={"orphan_orders": orphans, "layer": layer},
    )
