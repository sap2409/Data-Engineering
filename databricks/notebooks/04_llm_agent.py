# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — LLM quality agent
# MAGIC Reads the latest DQ snapshot and produces remediation advice.
# MAGIC Uses Databricks secret `openai` / `api_key` when available; otherwise offline mode.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "ecommerce_dq")
dbutils.widgets.text("secret_scope", "openai")
dbutils.widgets.text("secret_key", "api_key")
dbutils.widgets.text("openai_model", "gpt-4o-mini")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
secret_scope = dbutils.widgets.get("secret_scope")
secret_key = dbutils.widgets.get("secret_key")
openai_model = dbutils.widgets.get("openai_model")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

import json
from datetime import datetime, timezone

from pyspark.sql import functions as F

SYSTEM_PROMPT = """You are a senior data engineer specializing in medallion architecture
(Bronze → Silver → Gold) and data quality on Databricks.

You receive automated quality-check results comparing Bronze (raw) and Silver (cleaned) layers.
Explain failures clearly for an interview audience and recommend concrete Silver-layer fixes.

Respond in Markdown with:
1. Executive summary
2. Findings by table
3. Likely root causes
4. Recommended fixes (ordered by severity)
5. Suggested PySpark / Delta snippets
"""


def get_api_key():
    try:
        return dbutils.secrets.get(scope=secret_scope, key=secret_key)
    except Exception as exc:
        print(f"Secret not available ({exc}); using offline agent.")
        return ""


def analyze_offline(report: dict) -> str:
    lines = [
        "## Executive summary",
        "",
        f"Overall Bronze→Silver quality status: **{report.get('overall_status')}**.",
        "Offline mode — configure Databricks secret scope `openai`/`api_key` for live LLM analysis.",
        "",
        "## Findings by table",
        "",
    ]
    for table_report in report.get("tables", []):
        lines.append(f"### `{table_report['table']}` — {table_report['overall_status']}")
        for check in table_report.get("checks", []):
            if check["status"] != "PASS":
                lines.append(f"- **{check['status']}** `{check['name']}`: {check['message']}")
        lines.append("")
    lines.extend(
        [
            "## Recommended fixes",
            "",
            "1. Deduplicate on primary keys before Silver write.",
            "2. Quarantine invalid emails / blank required fields.",
            "3. Enforce `amount > 0` and allowed status enums.",
            "4. Inner-join orders to silver customers (quarantine orphans).",
            "5. Gate Gold jobs on `dq_results.overall_status = PASS`.",
        ]
    )
    return "\n".join(lines)


def analyze_with_llm(report: dict, api_key: str) -> str:
    # Prefer openai package if installed on cluster; fall back to REST.
    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=openai_model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": "Analyze these Bronze→Silver quality results:\n\n"
                    + json.dumps(report, indent=2),
                },
            ],
            temperature=0.2,
        )
        return resp.choices[0].message.content or "_Empty response_"
    except Exception:
        import urllib.request

        body = json.dumps(
            {
                "model": openai_model,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": "Analyze these Bronze→Silver quality results:\n\n"
                        + json.dumps(report, indent=2),
                    },
                ],
                "temperature": 0.2,
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            "https://api.openai.com/v1/chat/completions",
            data=body,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
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
    raise Exception("No dq_run_snapshots found. Run notebook 03_quality_checks first.")

run_id = latest[0]["run_id"]
report = json.loads(latest[0]["report_json"])
print("Analyzing run_id:", run_id, "overall:", report.get("overall_status"))

api_key = get_api_key().strip()
mode = "llm" if api_key else "offline"
try:
    analysis = analyze_with_llm(report, api_key) if api_key else analyze_offline(report)
except Exception as exc:
    mode = "offline_fallback"
    analysis = f"_LLM failed ({exc}). Offline analysis:_\n\n" + analyze_offline(report)

print(f"Agent mode: {mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent remediation report

# COMMAND ----------

displayHTML(f"<pre style='white-space:pre-wrap;font-family:Segoe UI,sans-serif'>{analysis}</pre>")

# COMMAND ----------

spark.createDataFrame(
    [
        (
            run_id,
            mode,
            analysis,
            report.get("overall_status"),
            datetime.now(timezone.utc),
        )
    ],
    ["run_id", "agent_mode", "analysis_md", "overall_status", "created_at"],
).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.{schema}.dq_agent_reports"
)

print(f"Saved agent report to {catalog}.{schema}.dq_agent_reports")
