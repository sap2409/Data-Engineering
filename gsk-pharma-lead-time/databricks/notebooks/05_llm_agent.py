# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — LLM / offline bottleneck agent
# MAGIC Reads latest DQ snapshot and explains E2E / D2D hotspots.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "mseg_lead_time")
dbutils.widgets.text("secret_scope", "openai")
dbutils.widgets.text("secret_key", "api_key")
dbutils.widgets.text("openai_model", "gpt-4o-mini")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
secret_scope = dbutils.widgets.get("secret_scope")
secret_key = dbutils.widgets.get("secret_key")
openai_model = dbutils.widgets.get("openai_model")

try:
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"USE SCHEMA {schema}")
except Exception:
    spark.sql(f"USE {schema}")
    catalog = "hive_metastore"

# COMMAND ----------

import json
from datetime import datetime, timezone

from pyspark.sql import functions as F

SYSTEM_PROMPT = """You are a senior pharma supply-chain data engineer.
You analyze batch lead times derived from SAP MSEG goods movements across:
supplier → procurement → inbound logistics → receipt → quality release →
manufacturing → packaging → finished goods → warehouse → DC → customer shipment → customer.

Explain Stage, D2D (handoff), and E2E bottlenecks for an interview audience.
Respond in Markdown with executive summary, slowest stages/handoffs, root causes, actions.
"""


def get_api_key():
    try:
        return dbutils.secrets.get(scope=secret_scope, key=secret_key)
    except Exception as exc:
        print(f"No secret ({exc}); offline mode.")
        return ""


def offline(report):
    kpi = report.get("kpi_summary", {})
    lines = [
        "## Executive summary",
        "",
        f"Overall: **{report.get('overall_status')}**.",
        f"Avg E2E: **{kpi.get('avg_e2e_days')}** days | Avg D2D: **{kpi.get('avg_d2d_days')}** days.",
        "Offline mode — add Databricks secret openai/api_key for live LLM.",
        "",
        "## Findings",
        "",
    ]
    for c in report.get("checks", []):
        if c["status"] != "PASS":
            lines.append(f"- **{c['status']}** `{c['name']}`: {c['message']}")
    lines.extend(["", "## Stage averages", ""])
    for stage, days in (kpi.get("avg_stage_days_by_stage") or {}).items():
        lines.append(f"- `{stage}`: {days}")
    lines.extend(
        [
            "",
            "## Recommended actions",
            "",
            "1. Attack longest D2D handoffs first (idle time between stages).",
            "2. Review quality_inspection_and_release dwell and retest loops.",
            "3. Tighten inbound logistics / receipt SLAs.",
            "4. Gate customer shipment on complete upstream stage coverage.",
            "5. Keep unmapped BWART in quarantine and expand mapping with MM consultants.",
        ]
    )
    return "\n".join(lines)


def llm(report, api_key):
    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=openai_model,
            temperature=0.2,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(report, indent=2)},
            ],
        )
        return resp.choices[0].message.content or "_Empty_"
    except Exception:
        import urllib.request

        body = json.dumps(
            {
                "model": openai_model,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(report, indent=2)},
                ],
                "temperature": 0.2,
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            "https://api.openai.com/v1/chat/completions",
            data=body,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        return payload["choices"][0]["message"]["content"]

# COMMAND ----------

latest = (
    spark.table(f"{catalog}.{schema}.dq_run_snapshots")
    .orderBy(F.desc("created_at"))
    .limit(1)
    .collect()
)
if not latest:
    raise Exception("Run notebook 04_quality_checks first.")

run_id = latest[0]["run_id"]
report = json.loads(latest[0]["report_json"])
api_key = get_api_key().strip()
mode = "llm" if api_key else "offline"
try:
    analysis = llm(report, api_key) if api_key else offline(report)
except Exception as exc:
    mode = "offline_fallback"
    analysis = f"_LLM failed ({exc})_\n\n" + offline(report)

print("run_id:", run_id, "| mode:", mode)
displayHTML(f"<pre style='white-space:pre-wrap'>{analysis}</pre>")

spark.createDataFrame(
    [(run_id, mode, analysis, report.get("overall_status"), datetime.now(timezone.utc))],
    ["run_id", "agent_mode", "analysis_md", "overall_status", "created_at"],
).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.{schema}.dq_agent_reports"
)
