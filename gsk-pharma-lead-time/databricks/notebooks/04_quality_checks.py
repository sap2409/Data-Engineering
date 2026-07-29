# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Quality checks
# MAGIC Validates MSEG reconciliation and lead-time KPI sanity. Appends `dq_results`.

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

import json
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

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
D2D_TARGET = 2.0
E2E_TARGET = 60.0


def result(name, status, message, **metrics):
    return {"name": name, "status": status, "message": message, "metrics": metrics}


bronze = spark.table(f"{catalog}.{schema}.bronze_mseg")
silver = spark.table(f"{catalog}.{schema}.silver_mseg")
quarantine = spark.table(f"{catalog}.{schema}.quarantine_mseg")
stage_events = spark.table(f"{catalog}.{schema}.silver_stage_events")
stage_lt = spark.table(f"{catalog}.{schema}.gold_stage_lead_times")
d2d_lt = spark.table(f"{catalog}.{schema}.gold_d2d_lead_times")
e2e_lt = spark.table(f"{catalog}.{schema}.gold_e2e_lead_times")

b, s, q = bronze.count(), silver.count(), quarantine.count()
checks = [
    result(
        "mseg_quarantine_reconciliation",
        "PASS" if b == s + q else "FAIL",
        f"Bronze={b}, Silver={s}, Quarantine={q}",
    )
]

se = stage_events.count()
dupes = se - stage_events.select("batch_id", "stage").distinct().count()
checks.append(
    result("unique_batch_stage", "PASS" if dupes == 0 else "FAIL", f"duplicate batch+stage: {dupes}")
)

bad_stage = stage_events.filter(~F.col("stage").isin(STAGES)).count()
checks.append(
    result("allowed_stages", "PASS" if bad_stage == 0 else "FAIL", f"invalid stages: {bad_stage}")
)

neg = (
    stage_lt.filter(F.col("stage_lead_time_days") < 0).count()
    + d2d_lt.filter(F.col("d2d_lead_time_days") < 0).count()
    + e2e_lt.filter(F.col("e2e_lead_time_days") < 0).count()
)
checks.append(
    result("non_negative_lead_times", "PASS" if neg == 0 else "FAIL", f"negative KPI rows: {neg}")
)

over_d2d = d2d_lt.filter(F.col("d2d_lead_time_days") > D2D_TARGET).count()
checks.append(
    result(
        "d2d_target_breach",
        "WARN" if over_d2d else "PASS",
        f"{over_d2d} handoff(s) above {D2D_TARGET} days",
    )
)
over_e2e = e2e_lt.filter(F.col("e2e_lead_time_days") > E2E_TARGET).count()
checks.append(
    result(
        "e2e_target_breach",
        "WARN" if over_e2e else "PASS",
        f"{over_e2e} batch(es) above {E2E_TARGET} days",
    )
)

statuses = {c["status"] for c in checks}
overall = "FAIL" if "FAIL" in statuses else ("WARN" if "WARN" in statuses else "PASS")

avg_e2e = e2e_lt.agg(F.round(F.avg("e2e_lead_time_days"), 2).alias("v")).collect()[0]["v"]
avg_d2d = d2d_lt.agg(F.round(F.avg("d2d_lead_time_days"), 2).alias("v")).collect()[0]["v"]
stage_avgs = {
    r["stage"]: r["avg_days"]
    for r in stage_lt.groupBy("stage")
    .agg(F.round(F.avg("stage_lead_time_days"), 2).alias("avg_days"))
    .collect()
}

report = {
    "overall_status": overall,
    "checks": checks,
    "kpi_summary": {
        "avg_e2e_days": float(avg_e2e) if avg_e2e is not None else None,
        "avg_d2d_days": float(avg_d2d) if avg_d2d is not None else None,
        "avg_stage_days_by_stage": stage_avgs,
        "batches_with_e2e": e2e_lt.count(),
        "d2d_handoffs": d2d_lt.count(),
    },
}

print("Overall:", overall)
display(spark.createDataFrame([(c["name"], c["status"], c["message"]) for c in checks], ["check", "status", "message"]))

# COMMAND ----------

run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
rows = [
    (
        run_id,
        c["name"],
        c["status"],
        c["message"],
        json.dumps(c.get("metrics", {})),
        overall,
        datetime.now(timezone.utc),
    )
    for c in checks
]
spark.createDataFrame(
    rows,
    ["run_id", "check_name", "status", "message", "metrics_json", "overall_status", "checked_at"],
).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.{schema}.dq_results"
)

spark.createDataFrame(
    [(run_id, json.dumps(report), overall, datetime.now(timezone.utc))],
    ["run_id", "report_json", "overall_status", "created_at"],
).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.{schema}.dq_run_snapshots"
)

dbutils.jobs.taskValues.set(key="run_id", value=run_id)
dbutils.jobs.taskValues.set(key="overall_status", value=overall)
print("Saved dq_results / dq_run_snapshots for run_id=", run_id)

if overall == "FAIL":
    raise Exception(f"Quality gate FAILED run_id={run_id}")
