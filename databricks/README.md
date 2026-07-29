# Databricks Bronze → Silver Quality Agent

Interview-ready port of the local PySpark project. Same logic: dirty Bronze → Silver + Quarantine → DQ checks → LLM remediation.

## Architecture

```
CSV / Volume  →  bronze_* (Delta)
                      ↓
              transform notebook
                      ↓
         silver_*  +  quarantine_* (Delta)
                      ↓
              quality checks → dq_results
                      ↓
              LLM agent (Databricks secret)
```

## Workspace setup (15–20 min)

1. Open a Databricks workspace (Free / Community / Trial).
2. **Repos → Create → Repo** and clone:
   `https://github.com/sap2409/Data-Engineering`
   (or upload the `databricks/notebooks/` folder).
3. Create an all-purpose cluster (Runtime 13.3 LTS+ / 14.x, Photon optional).
4. Open notebooks in order under `databricks/notebooks/`:
   - `00_setup_config`
   - `01_ingest_bronze`
   - `02_transform_silver`
   - `03_quality_checks`
   - `04_llm_agent`
5. On each notebook, attach the cluster. Set widgets if needed:
   - `catalog` → `main` (Unity Catalog) or `hive_metastore`
   - `schema` → `ecommerce_dq`

### Optional: OpenAI secret (LLM agent)

```bash
# In Databricks CLI / UI: Secrets → Create scope "openai"
databricks secrets create-scope openai
databricks secrets put-secret openai api_key
```

In the UI: **Settings → Secrets** (or User Settings → Developer → Access tokens for CLI).

Without a secret, notebook `04` still runs offline analysis.

### Optional Job

Import `databricks/jobs/bronze_silver_dq_job.yml` via **Workflows → Create → Job** (or Databricks Asset Bundles). Task order:

1. Ingest  
2. Transform  
3. Quality  
4. Agent  

## Interview talking points

See [INTERVIEW_DEMO.md](./INTERVIEW_DEMO.md).

## Tables created

| Table | Purpose |
|--------|---------|
| `{catalog}.{schema}.bronze_customers` | Raw customers |
| `{catalog}.{schema}.bronze_orders` | Raw orders |
| `{catalog}.{schema}.silver_customers` | Clean customers |
| `{catalog}.{schema}.silver_orders` | Clean orders |
| `{catalog}.{schema}.quarantine_customers` | Rejected customers + reason |
| `{catalog}.{schema}.quarantine_orders` | Rejected orders + reason |
| `{catalog}.{schema}.dq_results` | Check outcomes per run |
| `{catalog}.{schema}.dq_agent_reports` | LLM / offline narratives |
