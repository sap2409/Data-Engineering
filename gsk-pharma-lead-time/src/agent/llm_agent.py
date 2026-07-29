from __future__ import annotations

import json
import os
from typing import Any


SYSTEM_PROMPT = """You are a senior pharma supply-chain data engineer.
Lead times are derived from SAP MSEG (material document) goods movements.
Lifecycle stages in order:
supplier → raw_material_procurement → inbound_logistics → raw_material_receipt →
quality_inspection_and_release → manufacturing → packaging → finished_goods →
warehouse → distribution_center → customer_shipment → customer.

E2E = supplier start → customer end.
D2D = handoff wait between consecutive stages.
Explain bottlenecks clearly for an interview audience in Markdown.
"""


def analyze_lead_time_report(report: dict[str, Any]) -> str:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if api_key:
        try:
            return _llm(report, api_key)
        except Exception as exc:  # noqa: BLE001
            return f"_LLM call failed ({exc}). Offline analysis:_\n\n{_offline(report)}"
    return _offline(report)


def _llm(report: dict[str, Any], api_key: str) -> str:
    from openai import OpenAI

    client = OpenAI(api_key=api_key)
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    resp = client.chat.completions.create(
        model=model,
        temperature=0.2,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": "Analyze these MSEG-derived pharma lead-time results:\n\n"
                + json.dumps(report, indent=2),
            },
        ],
    )
    return resp.choices[0].message.content or "_Empty response_"


def _offline(report: dict[str, Any]) -> str:
    kpi = report.get("kpi_summary", {})
    lines = [
        "## Executive summary",
        "",
        f"Overall status: **{report.get('overall_status')}**.",
        "KPIs are built from synthetic SAP **MSEG** movements mapped by **BWART** to 12 lifecycle stages.",
        f"Average E2E (supplier→customer): **{kpi.get('avg_e2e_days')} days**.",
        f"Average D2D handoff: **{kpi.get('avg_d2d_days')} days**.",
        "",
        "## Findings",
        "",
    ]
    for check in report.get("checks", []):
        if check["status"] != "PASS":
            lines.append(f"- **{check['status']}** `{check['name']}`: {check['message']}")
    lines.extend(["", "## Stage averages (days)", ""])
    for stage, days in (kpi.get("avg_stage_days_by_stage") or {}).items():
        lines.append(f"- `{stage}`: {days}")
    lines.extend(
        [
            "",
            "## Recommended actions",
            "",
            "1. Prioritize longest D2D handoffs (idle time between stages).",
            "2. Review QI release (BWART 321/105) dwell for quality bottlenecks.",
            "3. Track inbound logistics (561/Z03) delays from customs/transport.",
            "4. Ensure every CHARG has full MSEG coverage supplier→customer for E2E.",
            "5. Keep unmapped BWART rows in quarantine and extend `bwart_stage_map`.",
        ]
    )
    return "\n".join(lines)
