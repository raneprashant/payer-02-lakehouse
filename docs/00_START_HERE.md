# Start here — Payer 02 Lakehouse

## Your mission
Build the Databricks Medallion pipelines **yourself**, one vertical slice at a time.
Do not paste code from the old project. Use specifications and expected results.

## First 20 minutes

1. Open the repository folder in VS Code.
2. Create `.venv` with Python 3.11, install dev dependencies, run `pytest -q`.
3. Verify your existing `DATABRICKS_FREE` profile and `databricks bundle validate -t dev --profile DATABRICKS_FREE`.
4. Run `sql/00_setup_dev_catalog.sql` in Databricks SQL Editor.
5. Upload **B0001 only** from your MEDIUM generator output into `payer_02_dev.landing.raw_files` (use `tools/upload_batch.ps1`).
6. Read `exercises/EX01_BRONZE_MEMBER_SNAPSHOT.md` and design the pipeline before writing code.

## Important context

- One Databricks Free Edition workspace, three catalog targets for CI/CD.
- All development/debugging happens in `payer_02_dev`.
- `payer_02_validation.source_qa` is prior source QA, not a Bronze or Silver target.
- `DATABRICKS_FREE` is a **Databricks CLI profile**; `SMOKE/MEDIUM/LARGE` are **Data Scales**, not connection profiles.
- In Stage 1 there is exactly one initial Snapshot per entity, then incremental CDC/event batches.
- Snapshot and CDC have **separate append-only Bronze tables** and checkpoints.
- Batch IDs and arrival dates are in flat per-entity filenames. Do not use arrival date as business CDC ordering.
- Do not copy all seven batches into DEV landing at once; release B0001, then B0002, and so on after exercises.
- No local Spark runtime is required. PySpark notebooks run in Databricks.

## Learning method

- Exercise 01: guided (I show structure and walk through the concepts).
- Exercise 02: you implement with review.
- Later exercises: instructions + expected results only.

Ask short questions freely; a question does not mean you want the full answer.
