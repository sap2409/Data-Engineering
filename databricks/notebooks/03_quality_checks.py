# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Quality checks (Bronze ↔ Silver + quarantine)
# MAGIC Enforces Silver contract + quarantine reconciliation. Appends history to `dq_results`.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "ecommerce_dq")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType


@dataclass
class CheckResult:
    name: str
    status: str
    message: str
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self):
        return asdict(self)


def check_quarantine_reconciliation(bronze, silver, quarantine) -> CheckResult:
    b, s, q = bronze.count(), silver.count(), quarantine.count()
    missing_reason = quarantine.filter(
        F.col("rejection_reason").isNull() | (F.trim(F.col("rejection_reason")) == "")
    ).count()
    ok = (s + q == b) and missing_reason == 0
    return CheckResult(
        "quarantine_reconciliation",
        "PASS" if ok else "FAIL",
        f"Bronze={b}, Silver={s}, Quarantine={q}, accounted={s + q}, blank_reasons={missing_reason}",
        {"bronze_count": b, "silver_count": s, "quarantine_count": q},
    )


def check_schema(bronze, silver, expected_bronze, expected_silver) -> CheckResult:
    bcols, scols = set(bronze.columns), set(silver.columns)
    expected_dropped = set(expected_bronze) - set(expected_silver)
    issues = []
    if set(expected_bronze) - bcols:
        issues.append(f"bronze missing: {sorted(set(expected_bronze) - bcols)}")
    if set(expected_silver) - scols:
        issues.append(f"silver missing: {sorted(set(expected_silver) - scols)}")
    unexpected = sorted((bcols - scols) - expected_dropped)
    if unexpected:
        issues.append(f"unexpected drops: {unexpected}")
    return CheckResult(
        "schema_drift",
        "FAIL" if issues else "PASS",
        "; ".join(issues) if issues else "Schemas align",
        {"dropped": sorted(bcols - scols)},
    )


def check_nulls(df: DataFrame, limits: dict, layer: str):
    total = df.count() or 1
    out = []
    for col, limit in limits.items():
        nulls = df.filter(F.col(col).isNull() | (F.trim(F.col(col).cast("string")) == "")).count()
        pct = nulls / total * 100
        out.append(
            CheckResult(
                f"null_pct_{layer}_{col}",
                "PASS" if pct <= limit else "FAIL",
                f"{layer}.{col}: {nulls}/{total} ({pct:.1f}%), limit={limit}%",
                {"null_pct": round(pct, 2)},
            )
        )
    return out


def check_uniqueness(df: DataFrame, columns: list, layer: str):
    total = df.count()
    out = []
    for col in columns:
        dupes = total - df.select(col).distinct().count()
        out.append(
            CheckResult(
                f"unique_{layer}_{col}",
                "PASS" if dupes == 0 else "FAIL",
                f"{layer}.{col}: {dupes} duplicate(s)",
                {"duplicates": dupes},
            )
        )
    return out


def check_email_format(df: DataFrame, layer="silver"):
    pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    bad = df.filter(~F.col("email").rlike(pattern) | F.col("email").isNull()).count()
    return CheckResult(
        f"email_format_{layer}",
        "PASS" if bad == 0 else "FAIL",
        f"{layer}: {bad} invalid email(s)",
        {"invalid_emails": bad},
    )


def check_amount_positive(df: DataFrame, layer="silver"):
    bad = df.filter(F.col("amount").isNull() | (F.col("amount").cast("double") <= 0)).count()
    return CheckResult(
        f"amount_positive_{layer}",
        "PASS" if bad == 0 else "FAIL",
        f"{layer}: {bad} non-positive/null amount(s)",
        {"bad_amounts": bad},
    )


def check_allowed_values(df: DataFrame, column: str, allowed: list, layer="silver"):
    bad = df.filter(~F.col(column).isin(allowed) | F.col(column).isNull()).count()
    return CheckResult(
        f"allowed_values_{layer}_{column}",
        "PASS" if bad == 0 else "FAIL",
        f"{layer}.{column}: {bad} outside {allowed}",
        {"bad_values": bad},
    )


def check_referential_integrity(orders, customers, layer="silver"):
    orphans = (
        orders.alias("o")
        .join(
            customers.alias("c"),
            F.col("o.customer_id") == F.col("c.customer_id"),
            "left_anti",
        )
        .count()
    )
    return CheckResult(
        f"referential_integrity_{layer}",
        "PASS" if orphans == 0 else "FAIL",
        f"{layer}: {orphans} orphan order(s)",
        {"orphan_orders": orphans},
    )


def overall(results):
    statuses = {r.status for r in results}
    if "FAIL" in statuses:
        return "FAIL"
    if "WARN" in statuses:
        return "WARN"
    return "PASS"

# COMMAND ----------

bronze_customers = spark.table(f"{catalog}.{schema}.bronze_customers")
silver_customers = spark.table(f"{catalog}.{schema}.silver_customers")
q_customers = spark.table(f"{catalog}.{schema}.quarantine_customers")
bronze_orders = spark.table(f"{catalog}.{schema}.bronze_orders")
silver_orders = spark.table(f"{catalog}.{schema}.silver_orders")
q_orders = spark.table(f"{catalog}.{schema}.quarantine_orders")

customer_checks = [
    check_quarantine_reconciliation(bronze_customers, silver_customers, q_customers),
    check_schema(
        bronze_customers,
        silver_customers,
        ["customer_id", "name", "email", "country", "signup_date", "raw_source"],
        ["customer_id", "name", "email", "country", "signup_date"],
    ),
    *check_nulls(silver_customers, {"email": 0, "name": 0, "country": 0}, "silver"),
    *check_uniqueness(silver_customers, ["customer_id", "email"], "silver"),
    check_email_format(silver_customers),
]

order_checks = [
    check_quarantine_reconciliation(bronze_orders, silver_orders, q_orders),
    check_schema(
        bronze_orders,
        silver_orders,
        ["order_id", "customer_id", "order_date", "amount", "currency", "status", "ingested_at"],
        ["order_id", "customer_id", "order_date", "amount", "currency", "status"],
    ),
    *check_nulls(silver_orders, {"order_id": 0, "customer_id": 0, "amount": 0, "status": 0}, "silver"),
    *check_uniqueness(silver_orders, ["order_id"], "silver"),
    check_amount_positive(silver_orders),
    check_allowed_values(
        silver_orders,
        "status",
        ["pending", "paid", "shipped", "cancelled", "refunded"],
    ),
]

cross_checks = [check_referential_integrity(silver_orders, silver_customers)]

report = {
    "overall_status": overall(customer_checks + order_checks + cross_checks),
    "tables": [
        {
            "table": "customers",
            "overall_status": overall(customer_checks),
            "checks": [c.to_dict() for c in customer_checks],
        },
        {
            "table": "orders",
            "overall_status": overall(order_checks),
            "checks": [c.to_dict() for c in order_checks],
        },
        {
            "table": "_cross_",
            "overall_status": overall(cross_checks),
            "checks": [c.to_dict() for c in cross_checks],
        },
    ],
}

print("Overall:", report["overall_status"])
for t in report["tables"]:
    print(f"  {t['table']}: {t['overall_status']}")

# COMMAND ----------

run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
rows = []
for t in report["tables"]:
    for c in t["checks"]:
        rows.append(
            (
                run_id,
                t["table"],
                c["name"],
                c["status"],
                c["message"],
                json.dumps(c.get("metrics", {})),
                report["overall_status"],
                datetime.now(timezone.utc),
            )
        )

dq_df = spark.createDataFrame(
    rows,
    schema=[
        "run_id",
        "table_name",
        "check_name",
        "status",
        "message",
        "metrics_json",
        "overall_status",
        "checked_at",
    ],
)

(
    dq_df.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.dq_results")
)

# Persist full report JSON for the agent notebook
spark.createDataFrame(
    [(run_id, json.dumps(report), report["overall_status"], datetime.now(timezone.utc))],
    ["run_id", "report_json", "overall_status", "created_at"],
).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.{schema}.dq_run_snapshots"
)

display(dq_df.orderBy("table_name", "check_name"))

# COMMAND ----------

# Fail the notebook/job when quality gate fails (interview talking point)
dbutils.jobs.taskValues.set(key="overall_status", value=report["overall_status"])
dbutils.jobs.taskValues.set(key="run_id", value=run_id)

if report["overall_status"] == "FAIL":
    raise Exception(f"Quality gate FAILED for run_id={run_id}")

print(f"Quality gate PASSED (run_id={run_id})")
