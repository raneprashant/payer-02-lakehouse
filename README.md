# Payer 02 Lakehouse — guided learning starter (v0.0.1)

This is a **starter repository, not a completed Medallion implementation**. You write the PySpark/SQL transformations to learn Databricks engineering. The separate data-generator repository is already completed and validated.

## Start here

1. Read [`docs/00_START_HERE.md`](docs/00_START_HERE.md).
2. Use [`CMD_README.md`](CMD_README.md) for brief Windows PowerShell commands.
3. Read [`exercises/EX01_BRONZE_MEMBER_SNAPSHOT.md`](exercises/EX01_BRONZE_MEMBER_SNAPSHOT.md).
4. Complete `src/notebooks/10_bronze_member_snapshot.py` and test on Databricks.

## What's in the starter?

- `databricks.yml` — separate `dev`, `test`, `prod` catalogs in **one** Databricks Free Edition workspace.
- `resources/00_environment_check.job.yml` — deployable safe, read-only job.
- `src/notebooks/` — two deliberately incomplete Bronze/Silver exercise notebooks.
- `src/payer_lakehouse/paths.py` — pure-Python, testable UC path conventions.
- `docs/SOURCE_CONTRACTS_REFERENCE.json` — 29 source entity contracts from generator v0.1.1.
- `sql/` — DEV environment setup and post-implementation validation queries.
- `tools/upload_batch.ps1` — upload B0001 then B0002, one batch at a time.
- `tests/` and `.github/workflows/python_ci.yml` — local and PR baseline tests.
- `exercises/` — mapping-driven exercise requirements and expected results.

## Hard constraints

- Write and debug data pipelines in `payer_02_dev`; TEST/PROD are CI/CD promotion targets.
- Use Databricks CLI profile `DATABRICKS_FREE` locally; do not commit credentials.
- No local Spark runtime or Databricks Connect requirement.
- Separate Snapshot and CDC Bronze tables; first build Snapshot ingestion.
- Source landing folder is flat per entity; filename has batch ID and arrival date.
- Future CDC processing must not use arrival date as business CDC ordering.
- No Stage 2 periodic snapshots yet.

## First exercise goal

Implement a replay-safe `member_master` **Snapshot → Bronze** ingestion through Auto Loader. Show why source rows, metadata, and file checkpoints matter. Do not start Silver before you can rerun Bronze without increasing its row count.

## Official references

- Databricks bundle configuration: https://docs.databricks.com/aws/en/dev-tools/bundles/settings
- Bundle getting started: https://docs.databricks.com/aws/en/dev-tools/bundles/jobs-tutorial
- Auto Loader with Unity Catalog: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/unity-catalog
