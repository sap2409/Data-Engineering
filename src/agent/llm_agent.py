from __future__ import annotations

import json
import os
from typing import Any


SYSTEM_PROMPT = """You are a senior data engineer specializing in medallion architecture
(Bronze → Silver → Gold) and data quality.

You receive automated quality-check results comparing Bronze (raw) and Silver (cleaned) layers.
Explain failures clearly for a learning audience and recommend concrete Silver-layer fixes.

Respond in Markdown with these sections:
1. Executive summary (2-4 sentences)
2. Findings by table (bullet list)
3. Likely root causes in the Bronze→Silver transform
4. Recommended fixes (actionable, ordered by severity)
5. Suggested Spark transform snippets (short PySpark examples where useful)
"""


def analyze_quality_report(report: dict[str, Any]) -> str:
    """Analyze DQ results via LLM when configured, else offline heuristics."""
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if api_key:
        try:
            return _analyze_with_llm(report, api_key)
        except Exception as exc:  # noqa: BLE001 - fall back for learning UX
            offline = _analyze_offline(report)
            return (
                f"_LLM call failed ({exc}). Showing offline analysis instead._\n\n{offline}"
            )
    return _analyze_offline(report)


def _analyze_with_llm(report: dict[str, Any], api_key: str) -> str:
    from openai import OpenAI

    client_kwargs: dict[str, Any] = {"api_key": api_key}
    base_url = os.getenv("OPENAI_BASE_URL", "").strip()
    if base_url:
        client_kwargs["base_url"] = base_url

    client = OpenAI(**client_kwargs)
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    user_content = (
        "Analyze these Bronze→Silver quality results and advise on fixes:\n\n"
        f"```json\n{json.dumps(report, indent=2)}\n```"
    )

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        temperature=0.2,
    )
    return response.choices[0].message.content or "_Empty LLM response_"


def _analyze_offline(report: dict[str, Any]) -> str:
    """Heuristic narrative so the project works without an API key."""
    overall = report.get("overall_status", "UNKNOWN")
    lines = [
        "## Executive summary",
        "",
        f"Overall Bronze→Silver quality status: **{overall}**.",
        "Sample data intentionally includes duplicates, nulls, invalid emails,",
        "negative amounts, bad statuses, and orphan orders so you can practice DQ.",
        "Set `OPENAI_API_KEY` in `.env` for richer LLM analysis.",
        "",
        "## Findings by table",
        "",
    ]

    for table_report in report.get("tables", []):
        table = table_report.get("table", "?")
        status = table_report.get("overall_status", "?")
        lines.append(f"### `{table}` — {status}")
        for check in table_report.get("checks", []):
            if check["status"] == "PASS":
                continue
            lines.append(f"- **{check['status']}** `{check['name']}`: {check['message']}")
        lines.append("")

    lines.extend(
        [
            "## Likely root causes",
            "",
            "- Bronze retains raw ingress issues (duplicates, blanks, invalid enums).",
            "- Silver transforms drop bad rows but may exceed `max_row_drop_pct`.",
            "- Metadata columns (`raw_source`, `ingested_at`) are intentionally removed in Silver.",
            "- Orphan `customer_id` values indicate missing referential cleanup.",
            "",
            "## Recommended fixes",
            "",
            "1. Deduplicate Bronze on primary keys before writing Silver.",
            "2. Reject or quarantine invalid emails / blank required fields.",
            "3. Filter `amount > 0` and constrain `status` to the allowed set.",
            "4. Inner-join orders to customers (or quarantine orphans).",
            "5. Track rejected rows in a quarantine table for auditability.",
            "",
            "## Suggested Spark transform snippets",
            "",
            "```python",
            "# Deduplicate + basic customer cleaning",
            "from pyspark.sql import functions as F",
            "",
            "silver_customers = (",
            "    bronze_customers",
            "    .dropDuplicates(['customer_id'])",
            "    .filter(F.col('email').rlike(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'))",
            "    .filter(F.col('name').isNotNull() & (F.trim(F.col('name')) != ''))",
            "    .fillna({'country': 'UNKNOWN'})",
            "    .drop('raw_source')",
            ")",
            "",
            "silver_orders = (",
            "    bronze_orders",
            "    .dropDuplicates(['order_id'])",
            "    .filter(F.col('amount').cast('double') > 0)",
            "    .filter(F.col('status').isin('pending','paid','shipped','cancelled','refunded'))",
            "    .join(silver_customers.select('customer_id'), 'customer_id', 'inner')",
            "    .drop('ingested_at')",
            ")",
            "```",
        ]
    )
    return "\n".join(lines)
