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