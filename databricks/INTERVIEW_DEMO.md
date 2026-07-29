# Interview demo script (8–10 minutes)

Use this while screen-sharing Databricks.

## 0. Setup (before panel joins)

- Cluster running  
- Notebooks attached  
- Run `00` → `01` → `02` → `03` once so tables exist  
- Keep `04` ready (optional secret configured)

## 1. Pitch (45 seconds)

> “This is a medallion Bronze→Silver quality pipeline on Databricks. Dirty ecommerce data lands in Bronze Delta tables. A PySpark transform writes clean Silver and quarantines rejects with reasons. Automated DQ validates reconciliation, uniqueness, nulls, enums, and referential integrity. Results are logged, and an LLM agent explains failures and suggests Silver fixes.”

## 2. Show dirty Bronze (1 min)

Run in a SQL cell or notebook:

```sql
SELECT * FROM main.ecommerce_dq.bronze_customers;
SELECT * FROM main.ecommerce_dq.bronze_orders;
```

Call out: duplicate `C002` / `O1002`, blank email, invalid status `UNKNOWN`, orphan `C999`.

## 3. Transform + Quarantine (2 min)

Open `02_transform_silver`, run all, show counts:

- customers: 13 → 9 silver + 4 quarantine  
- orders: 13 → 7 silver + 6 quarantine  

```sql
SELECT rejection_reason, count(*) AS n
FROM main.ecommerce_dq.quarantine_orders
GROUP BY rejection_reason
ORDER BY n DESC;
```

## 4. Quality gate (2 min)

Open `03_quality_checks`, show **Overall: PASS** and `dq_results` table.

```sql
SELECT table_name, check_name, status, message
FROM main.ecommerce_dq.dq_results
WHERE run_id = (SELECT max(run_id) FROM main.ecommerce_dq.dq_results)
ORDER BY table_name, check_name;
```

Mention: Job would fail if overall status is FAIL (gate before Gold).

## 5. LLM agent (1–2 min)

Open `04_llm_agent`, run, show remediation narrative.

If no API key: “Offline mode uses the same structure; production uses a secret scope.”

## 6. Closing (30 seconds)

> “Same design works locally with files and here with Delta + Jobs + secrets. Next step would be Gold aggregations and DLT expectations.”

## Likely panel questions → short answers

| Question | Answer |
|----------|--------|
| Why quarantine? | Auditability; don’t silently drop rows |
| Why not only dropDuplicates? | Business rules (email, amount, FK) need explicit rejects |
| How do you schedule? | Databricks Job: ingest → transform → DQ → agent |
| How secrets? | `dbutils.secrets.get("openai", "api_key")` |
| Scale? | Same Spark code; cluster size / Photon / partitioning |
| Idempotency? | Overwrite Silver/quarantine per batch; append DQ history |
