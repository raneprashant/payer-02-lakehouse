# Command cheat sheet (Windows PowerShell)

Run these in the `payer-02-lakehouse` root unless stated otherwise.

## Local setup (Python 3.11; no local Spark required)

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pytest -q
```
Creates local virtual environment, installs pure Python dev/test tooling, runs tests.

## Confirm Databricks CLI connection and bundle targets

```powershell
databricks auth profiles
databricks bundle validate --target dev --profile DATABRICKS_FREE
databricks bundle validate --target test --profile DATABRICKS_FREE
databricks bundle validate --target prod --profile DATABRICKS_FREE
```
Only validates bundle config; DEV/TEST/PROD all resolve to the same workspace but different catalogs.

## Deploy and run safe environment-check job (optional)

```powershell
databricks bundle deploy --target dev --profile DATABRICKS_FREE
databricks bundle run --target dev environment_check --profile DATABRICKS_FREE
```
This read-only job checks mapping for `payer_02_dev`; it does not create Bronze/Silver tables.

## Prepare DEV schemas and volumes

Open `sql/00_setup_dev_catalog.sql` in Databricks SQL Editor and run it **once**.

## Upload ONLY B0001 (Snapshot) to DEV landing (PowerShell)

```powershell
.\tools\upload_batch.ps1 -GeneratorRoot "C:\Projects\payer-02-data-generator" -Scale MEDIUM -Batch B0001
```
Uploads files for one batch; never copy all CDC batches into DEV before learning incremental ingestion.

## Later, after the pipeline is tested, upload next batch

```powershell
.\tools\upload_batch.ps1 -GeneratorRoot "C:\Projects\payer-02-data-generator" -Scale MEDIUM -Batch B0002
```
Do this only when we are ready to study the first CDC scenarios.

## Git repository setup (new local repo)

```powershell
git init -b develop
git add .
git commit -m "Initial Payer 02 Lakehouse starter framework"
```
Create an empty private GitHub repository, then add remote and push `develop`.

```powershell
git remote add origin https://github.com/<YOUR_USERNAME>/payer-02-lakehouse.git
git push -u origin develop
```

## Read source contract and start coding

```powershell
python tools/check_contracts.py
```
Read `exercises/EX01_BRONZE_MEMBER_SNAPSHOT.md`. Write TODOs in `src/notebooks/10_bronze_member_snapshot.py` yourself.
