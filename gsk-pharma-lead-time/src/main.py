from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.agent.llm_agent import analyze_lead_time_report
from src.analytics.lead_times import run_lead_time_analytics
from src.quality.checks import run_quality_checks
from src.spark_session import get_spark
from src.transform.bronze_to_silver import run_bronze_to_silver

console = Console()


def _style(status: str) -> str:
    return {"PASS": "green", "WARN": "yellow", "FAIL": "red"}.get(status, "white")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="SAP MSEG → 12-stage pharma E2E/D2D lead-time pipeline"
    )
    parser.add_argument("--config", default=str(ROOT / "config" / "lead_time_rules.yaml"))
    parser.add_argument("--reports-dir", default=str(ROOT / "reports"))
    parser.add_argument("--transform", action="store_true")
    parser.add_argument("--skip-agent", action="store_true")
    args = parser.parse_args()

    env_path = ROOT / ".env"
    if not env_path.exists() and (ROOT / ".env.example").exists():
        env_path.write_text((ROOT / ".env.example").read_text(encoding="utf-8"), encoding="utf-8")
    load_dotenv(env_path)

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    if args.transform:
        console.print("[bold]Bronze MSEG -> Silver (cleaned MSEG + stage events)...[/bold]")
        summary = run_bronze_to_silver(spark, args.config)
        console.print(
            f"MSEG Bronze={summary['bronze']} | Silver lines={summary['silver_mseg']} | "
            f"Stage events={summary['silver_stages']} | Quarantine={summary['quarantine']}"
        )

    console.print("[bold]Gold KPIs: stage / D2D / E2E lead times...[/bold]")
    gold = run_lead_time_analytics(spark, args.config)
    console.print(
        f"Stage rows={gold['stage_rows']} | D2D={gold['d2d_rows']} | E2E={gold['e2e_rows']}"
    )

    console.print("[bold]Quality + target checks...[/bold]")
    report = run_quality_checks(spark, args.config)

    table = Table(title="MSEG lead-time quality summary")
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Message")
    for c in report["checks"]:
        table.add_row(c["name"], f"[{_style(c['status'])}]{c['status']}[/]", c["message"])
    console.print(table)
    console.print(
        Panel(
            f"Overall: [{_style(report['overall_status'])}]{report['overall_status']}[/]",
            title="Result",
        )
    )
    kpi = report["kpi_summary"]
    console.print(
        f"Avg E2E={kpi.get('avg_e2e_days')} days | Avg D2D={kpi.get('avg_d2d_days')} days"
    )

    analysis = ""
    if not args.skip_agent:
        mode = "LLM" if os.getenv("OPENAI_API_KEY", "").strip() else "Offline"
        console.print(f"\n[bold]Agent analysis ({mode})...[/bold]")
        analysis = analyze_lead_time_report(report)
        try:
            console.print(Markdown(analysis))
        except UnicodeEncodeError:
            console.print(analysis)

    out = Path(args.reports_dir)
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    payload = {**report, "gold": gold, "agent_analysis": analysis}
    json_path = out / f"lead_time_report_{stamp}.json"
    md_path = out / f"agent_analysis_{stamp}.md"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    md_path.write_text(analysis, encoding="utf-8")
    console.print(f"\nSaved -> {json_path}")

    spark.stop()
    return 0 if report["overall_status"] != "FAIL" else 1


if __name__ == "__main__":
    raise SystemExit(main())
