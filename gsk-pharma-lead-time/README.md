# Pharma Lead Time Analytics from SAP MSEG (Demo)

Synthetic / interview sample only — **not affiliated with GSK** and **not real SAP production data**.

## What this project does

Reads **SAP MSEG-style material document lines** (goods movements), maps **BWART** (movement type) to a **12-stage** product lifecycle, then computes:

| Metric | Meaning |
|--------|---------|
| **Stage lead time** | Days from first→last MSEG timestamp in that stage (`CHARG` + stage) |
| **D2D lead time** | Handoff wait: end(stage N) → start(stage N+1) |
| **E2E lead time** | Supplier start → Customer end |

```
Bronze MSEG
  → quarantine invalid lines
  → Silver cleaned MSEG + stage events
  → Gold stage / D2D / E2E KPIs
  → LLM / offline bottleneck analysis
```

## 12-stage sequence

1. `supplier` — Supplier  
2. `raw_material_procurement` — Raw material procurement  
3. `inbound_logistics` — Inbound logistics  
4. `raw_material_receipt` — Raw material receipt  
5. `quality_inspection_and_release` — Quality inspection & release  
6. `manufacturing` — Manufacturing  
7. `packaging` — Packaging  
8. `finished_goods` — Finished goods  
9. `warehouse` — Warehouse  
10. `distribution_center` — Distribution center  
11. `customer_shipment` — Customer shipment  
12. `customer` — Customer  

## SAP MSEG field mapping (demo)

| MSEG field | Role in pipeline |
|------------|------------------|
| `MBLNR` + `MJAHR` + `ZEILE` | Material document key (dedupe) |
| `MATNR` | Material |
| `WERKS` | Plant |
| `CHARG` | Batch → `batch_id` for lead times |
| `BWART` | Movement type → **stage** via `bwart_stage_map` |
| `MENGE` / `MEINS` | Quantity / UoM |
| `BUDAT` / `CPUDT_TIME` | Event time (`CPUDT_TIME` preferred) |
| `LGORT` | Storage location |
| `LIFNR` | Vendor (supplier-side) |
| `KUNNR` | Customer |
| `AUFNR` / `VBELN` | Order / delivery refs (context) |

## Example BWART → stage rules

Configured in `config/lead_time_rules.yaml` (`bwart_stage_map`):

| BWART | Stage |
|-------|--------|
| `Z01` | supplier |
| `Z02` / `541` | raw_material_procurement |
| `Z03` / `561` | inbound_logistics |
| `101` / `103` | raw_material_receipt |
| `321` / `105` / `322` | quality_inspection_and_release |
| `261` / `101F` | manufacturing |
| `Z04` / `309` | packaging |
| `501` / `Z05` | finished_goods |
| `311` / `315` | warehouse |
| `641` / `Z07` | distribution_center |
| `601` | customer_shipment |
| `Z06` | customer |

Note: PO GR uses `101`; DC receipt uses demo `Z07` so BWART `101` is not ambiguous.
Unmapped BWART → **quarantine** (`unmapped_bwart`).

## Run

```powershell
cd "C:\Users\sunil\Quality check between layers\gsk-pharma-lead-time"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:PYTHONIOENCODING = "utf-8"
python -m src.main --transform
```

Optional: copy `.env.example` → `.env` and set `OPENAI_API_KEY` (needs API billing quota).

## Outputs

| Path | Content |
|------|---------|
| `data/silver/mseg_cleaned.csv` | Valid MSEG + stage + event_ts |
| `data/silver/batch_stage_events.csv` | One row per batch+stage |
| `data/quarantine/mseg_rejected.csv` | Rejected MSEG + reason |
| `data/gold/stage_lead_times.csv` | Stage KPIs |
| `data/gold/d2d_lead_times.csv` | Handoff KPIs |
| `data/gold/e2e_lead_times.csv` | End-to-end KPIs |
| `reports/` | JSON + agent narrative |

## Interview one-liner

> “We derive lifecycle lead times from SAP MSEG goods movements: BWART maps each posting to a supply-chain stage, we aggregate by batch, then measure stage duration, gate-to-gate D2D handoffs, and supplier-to-customer E2E — with quarantine for bad postings.”

## Learn the project (plain English)

Read **[`PROJECT_EXPLAINER.txt`](./PROJECT_EXPLAINER.txt)** — stages, MSEG fields, E2E/D2D meaning, how to run, interview talking points.

## Databricks version (importable)

See **[`databricks/`](./databricks/)**:

| Notebook | Purpose |
|----------|---------|
| `00_setup_config` | Schema setup |
| `01_ingest_mseg_bronze` | MSEG → Bronze Delta |
| `02_transform_silver` | Clean + quarantine + stage events |
| `03_lead_time_gold` | Stage / D2D / E2E KPIs |
| `04_quality_checks` | Quality gate |
| `05_llm_agent` | Bottleneck narrative |

Import steps: [`databricks/README.md`](./databricks/README.md)
