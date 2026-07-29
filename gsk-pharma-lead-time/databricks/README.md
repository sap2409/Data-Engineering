# Databricks — MSEG Lead Time Analytics

Import these notebooks into Databricks (Repos clone or Workspace import).

## Import options

### Option A — Git folder / Repo (recommended)
1. In Databricks: **Repos → Add Repo**
2. Clone `https://github.com/sap2409/Data-Engineering` (or your fork)
3. Open `gsk-pharma-lead-time/databricks/notebooks/`
4. Attach a cluster and run **00 → 05**

### Option B — Import notebook files
1. Workspace → Import
2. Upload each `*.py` from `databricks/notebooks/` (Databricks source format)
3. Also upload `../data/bronze/mseg_material_documents.csv` to a **Volume** or Workspace file
4. Set widget `bronze_csv` on notebook `01` to that path

## Widgets

| Widget | Default | Purpose |
|--------|---------|---------|
| `catalog` | `main` | Unity Catalog (or use hive fallback) |
| `schema` | `mseg_lead_time` | Schema/database name |
| `bronze_csv` | (auto from Repo) | Path to MSEG CSV if not using Repo file |

## Notebook order

1. `00_setup_config` — create schema / table names  
2. `01_ingest_mseg_bronze` — load MSEG → Delta bronze  
3. `02_transform_silver` — clean, quarantine, stage events  
4. `03_lead_time_gold` — stage / D2D / E2E KPIs  
5. `04_quality_checks` — quality gate + `dq_results`  
6. `05_llm_agent` — bottleneck narrative (secret optional)

## Tables created

- `{catalog}.{schema}.bronze_mseg`
- `{catalog}.{schema}.silver_mseg`
- `{catalog}.{schema}.silver_stage_events`
- `{catalog}.{schema}.quarantine_mseg`
- `{catalog}.{schema}.gold_stage_lead_times`
- `{catalog}.{schema}.gold_d2d_lead_times`
- `{catalog}.{schema}.gold_e2e_lead_times`
- `{catalog}.{schema}.dq_results`
- `{catalog}.{schema}.dq_run_snapshots`
- `{catalog}.{schema}.dq_agent_reports`

## Optional OpenAI secret

```text
scope: openai
key:   api_key
```

Without it, notebook `05` runs offline analysis.
