# Bronze → Silver Quality Check Agent

Local learning project: **PySpark** quality checks between medallion **Bronze** and **Silver** layers, plus an **LLM agent** that explains failures and suggests Silver transforms.

## Architecture

```
data/bronze/*.csv  ──┐
                     ├──► PySpark DQ engine ──► JSON report ──► LLM agent ──► Markdown advice
data/silver/*.csv  ──┘
```

Sample domain: **customers** + **orders** (ecommerce). Bronze is intentionally dirty; Silver is partially cleaned so checks have real findings.

## What gets checked

| Check | Purpose |
|--------|---------|
| Row count delta | How many rows were dropped Bronze→Silver |
| Schema drift | Columns dropped/added vs expected |
| Null / blank rates | Required fields in Silver |
| Uniqueness | Primary keys / emails |
| Email format | Regex validation |
| Amount > 0 | Order amounts |
| Allowed status | Enum validation |
| Referential integrity | Orders → customers |

## Setup

```bash
# From project root
python -m venv .venv

# Windows PowerShell
.\.venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

**Java:** PySpark needs a JDK (17 recommended). If Spark fails to start, install Temurin/OpenJDK and set `JAVA_HOME`.

**Note:** This project clears `SPARK_HOME` at runtime so the pip `pyspark==3.5.5` jars are used (avoids conflicts with a separate Spark install).

Optional LLM mode:

```bash
copy .env.example .env
# Edit .env and set OPENAI_API_KEY=...
```

Without a key, the agent uses built-in offline analysis (still useful for learning).

## Run

Transform Bronze → Silver (writes cleaned silver + quarantine), then quality-check:

```bash
python -m src.main --transform
```

Checks only (existing silver/quarantine):

```bash
python -m src.main
```

Transform only:

```bash
python -m src.main --transform-only
```

Useful flags:

```bash
python -m src.main --transform --skip-agent
python -m src.main --config config/quality_rules.yaml
```

Reports land in `reports/`:

- `quality_report_*.json` — machine-readable check results
- `agent_analysis_*.md` — LLM or offline narrative

Quarantine rejected rows land in `data/quarantine/`.

## LLM API key

1. `.env` is created automatically from `.env.example` on first run.
2. Edit `.env` and set:

```env
OPENAI_API_KEY=sk-your-key-here
OPENAI_MODEL=gpt-4o-mini
```

3. Re-run `python -m src.main --transform`

Without a key, the agent still runs in offline mode.
## Environments & CI/CD (Dev → UAT → Prod)

Multi-env Databricks Asset Bundle + GitHub Actions with **manual Prod approval**:

- Bundle: [`databricks.yml`](./databricks.yml)
- Jobs: [`resources/jobs/`](./resources/jobs/)
- Env configs: [`config/environments/`](./config/environments/)
- Workflow: [`.github/workflows/cicd.yml`](./.github/workflows/cicd.yml)
- Full guide: [`docs/CICD_AND_ENVIRONMENTS.md`](./docs/CICD_AND_ENVIRONMENTS.md)

## Databricks interview version

Ready-to-run notebooks live in [`databricks/`](./databricks/):

- Ingest → Transform → Quality → LLM agent  
- Delta tables + quarantine + `dq_results`  
- Demo script: [`databricks/INTERVIEW_DEMO.md`](./databricks/INTERVIEW_DEMO.md)

```text
databricks/notebooks/00_setup_config.py
databricks/notebooks/01_ingest_bronze.py
databricks/notebooks/02_transform_silver.py
databricks/notebooks/03_quality_checks.py
databricks/notebooks/04_llm_agent.py
```

See [`databricks/README.md`](./databricks/README.md) for workspace setup.

## Related: GSK-style pharma lead-time project

Sibling demo under [`gsk-pharma-lead-time/`](./gsk-pharma-lead-time/) for **E2E / D2D lead times** from **SAP MSEG**, across 12 stages (supplier → customer).

- Explainer: [`gsk-pharma-lead-time/PROJECT_EXPLAINER.txt`](./gsk-pharma-lead-time/PROJECT_EXPLAINER.txt)
- Databricks import: [`gsk-pharma-lead-time/databricks/`](./gsk-pharma-lead-time/databricks/)

## Project layout

```
config/quality_rules.yaml   # thresholds & table contracts
data/bronze/                # raw sample CSVs
data/silver/                # cleaned layer (written by transform)
data/quarantine/            # rejected rows + rejection_reason
src/transform/              # Bronze→Silver PySpark job
src/quality/                # PySpark checks + runner
src/agent/                  # LLM / offline analyst
src/main.py                 # CLI
reports/                    # generated outputs
```

## Learning path

1. Inspect dirty Bronze CSVs under `data/bronze/`.
2. Run `python -m src.main --transform` — Silver + quarantine are rewritten, then checks run.
3. Open `data/quarantine/*.csv` to see rejection reasons.
4. Add `OPENAI_API_KEY` to `.env` for richer remediation advice.
5. Tweak rules in `config/quality_rules.yaml` and re-run.