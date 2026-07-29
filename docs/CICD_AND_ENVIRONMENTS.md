# Dev / UAT / Prod + CI/CD guide

## What was added

| File | Purpose |
|------|---------|
| `databricks.yml` | Asset Bundle with targets `dev`, `uat`, `prod` |
| `resources/jobs/*.yml` | Env-parameterized Databricks Jobs |
| `config/environments/*.yml` | Human-readable env settings |
| `.github/workflows/cicd.yml` | CI tests + CD with **Prod approval** |

## Environment model

| Env | Catalog | OpenAI secret scope | Deploy trigger |
|-----|---------|---------------------|----------------|
| DEV | `dev` | `openai-dev` | push to `develop` |
| UAT | `uat` | `openai-uat` | push to `main` |
| PROD | `prod` | `openai-prod` | push to `main` **after human approval** |

Same notebooks/jobs; only variables change (`catalog`, secrets, workers).

## One-time GitHub setup

1. Create branches if missing: `develop`, `main`
2. Repo → **Settings → Environments** → create:
   - `dev`
   - `uat`
   - `production`
3. On **`production`**:
   - **Required reviewers** → add yourself / lead
4. For each environment, add secrets:
   - `DATABRICKS_HOST` = `https://<workspace>.cloud.databricks.com`
   - `DATABRICKS_TOKEN` = PAT with job/repo deploy rights

## One-time Databricks setup

1. Edit `databricks.yml` → replace `YOUR-*-WORKSPACE` hosts
2. Create catalogs/schemas (or let notebooks `CREATE SCHEMA`):
   - `dev.ecommerce_dq`, `dev.mseg_lead_time`
   - `uat.*`, `prod.*`
3. Create secret scopes:
   ```bash
   databricks secrets create-scope openai-dev
   databricks secrets put-secret openai-dev api_key
   # repeat for openai-uat, openai-prod
   ```

## Local deploy (optional)

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev bronze_silver_dq_agent
databricks bundle run -t dev mseg_lead_time_agent
```

## Promotion flow

```text
feature/*  --PR-->  develop  --auto deploy-->  DEV
                       |
                       PR
                       v
                     main  --auto deploy-->  UAT
                       |
                       CD waits for Approver
                       v
                     PROD deploy
```

1. Develop & test on a feature branch  
2. Merge to `develop` → CI + DEV deploy  
3. Validate in DEV  
4. Merge `develop` → `main` → CI + UAT deploy  
5. Business/UAT sign-off  
6. In GitHub Actions, **Approve** the `production` environment → PROD deploy  

## Two different “go / no-go” gates

1. **Release gate** — human approval before Prod code deploy (this CI/CD)  
2. **Data gate** — pipeline quality checks PASS/WARN/FAIL inside each env Job  

## Interview one-liner

> “We use one Databricks Asset Bundle with dev/uat/prod targets. GitHub Actions deploys develop→DEV and main→UAT automatically; production uses a GitHub Environment with required reviewers so no one ships to prod without approval.”
