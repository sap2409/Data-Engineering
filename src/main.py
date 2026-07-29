from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Avoid Windows cp1252 crashes on Unicode in Rich/Markdown output.
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

from src.agent.llm_agent import analyze_quality_report
from src.quality.runner import run_all_checks
from src.spark_session import get_spark
from src.transform.bronze_to_silver import run_bronze_to_silver

console = Console()


def _status_style(status: str) -> str:
    return {"PASS": "green", "WARN": "yellow", "FAIL": "red"}.get(status, "white")


def print_transform_summary(summary: dict) -> None:
    table = Table(title="Bronze -> Silver Transform")
    table.add_column("Table")
    table.add_column("Bronze")
    table.add_column("Silver")
    table.add_column("Quarantine")

    for name, stats in summary.get("tables", {}).items():
        table.add_row(
            name,
            str(stats["bronze"]),
            str(stats["silver"]),
            str(stats["quarantine"]),
        )
    console.print(table)


def print_summary(report: dict) -> None:
    table = Table(title="Bronze -> Silver Quality Summary")
    table.add_column("Table")
    table.add_column("Status")
    table.add_column("Failed / Warned checks")

    for t in report["tables"]:
        bad = [c for c in t.get("checks", []) if c["status"] != "PASS"]
        table.add_row(
            t["table"],
            f"[{_status_style(t['overall_status'])}]{t['overall_status']}[/]",
            str(len(bad)),
        )

    console.print(table)
    console.print(
        Panel(
            f"Overall: [{_status_style(report['overall_status'])}]{report['overall_status']}[/]",
            title="Result",
        )
    )


def write_reports(report: dict, analysis: str, out_dir: Path) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_path = out_dir / f"quality_report_{stamp}.json"
    md_path = out_dir / f"agent_analysis_{stamp}.md"

    payload = {**report, "agent_analysis": analysis}
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    md_path.write_text(analysis, encoding="utf-8")
    return json_path, md_path


def ensure_env_file() -> Path:
    env_path = ROOT / ".env"
    example = ROOT / ".env.example"
    if not env_path.exists() and example.exists():
        env_path.write_text(example.read_text(encoding="utf-8"), encoding="utf-8")
        console.print(f"Created {env_path} from .env.example")
    return env_path


def agent_mode_banner() -> None:
    if os.getenv("OPENAI_API_KEY", "").strip():
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        console.print(f"[green]LLM agent mode[/green] (model={model})")
    else:
        console.print(
            "[yellow]Offline agent mode[/yellow] — set OPENAI_API_KEY in .env for richer remediation"
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Bronze→Silver transform, quality checks, and LLM analysis"
    )
    parser.add_argument(
        "--config",
        default=str(ROOT / "config" / "quality_rules.yaml"),
        help="Path to quality rules YAML",
    )
    parser.add_argument(
        "--reports-dir",
        default=str(ROOT / "reports"),
        help="Directory for JSON/Markdown reports",
    )
    parser.add_argument(
        "--transform",
        action="store_true",
        help="Run Bronze→Silver transform (writes silver + quarantine) before checks",
    )
    parser.add_argument(
        "--transform-only",
        action="store_true",
        help="Only run the transform; skip quality checks and agent",
    )
    parser.add_argument(
        "--skip-agent",
        action="store_true",
        help="Only run PySpark checks; skip LLM/offline analysis",
    )
    args = parser.parse_args()

    ensure_env_file()
    load_dotenv(ROOT / ".env")

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    if args.transform or args.transform_only:
        console.print("[bold]Running Bronze -> Silver transform...[/bold]")
        summary = run_bronze_to_silver(spark, args.config)
        print_transform_summary(summary)
        if args.transform_only:
            spark.stop()
            return 0

    console.print("[bold]Running Bronze -> Silver quality checks...[/bold]")
    report = run_all_checks(spark, args.config)
    print_summary(report)

    analysis = ""
    if not args.skip_agent:
        agent_mode_banner()
        console.print("\n[bold]Agent analysis...[/bold]")
        analysis = analyze_quality_report(report)
        try:
            console.print(Markdown(analysis))
        except UnicodeEncodeError:
            console.print(analysis)

    json_path, md_path = write_reports(report, analysis, Path(args.reports_dir))
    console.print(f"\nSaved JSON report -> {json_path}")
    if analysis:
        console.print(f"Saved agent analysis -> {md_path}")

    spark.stop()
    return 0 if report["overall_status"] != "FAIL" else 1


if __name__ == "__main__":
    raise SystemExit(main())
