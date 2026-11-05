# Exercise 01 — Member Snapshot → Bronze (guided)

**Source contract:** `docs/SOURCE_CONTRACTS_REFERENCE.json` → `sources.member_master`

**Inputs:** `payer-02-data-generator/output/MEDIUM/landing/member_master/member_master_snapshot_B0001_*.parquet`

**Databricks landing:** `/Volumes/payer_02_dev/landing/raw_files/landing/member_master/`

**Target:** `payer_02_dev.bronze.member_master_snapshot` (append-only)

## Requirements (do not code yet)

1. Explain **grain**: one incoming source Snapshot record, not one cleaned member.
2. Inspect columns and types; refer to source contract. Preserve all source columns and rows.
3. Read only `member_master_snapshot_*.parquet`; the same directory will later contain `member_master_cdc_*.parquet`.
4. Use **Auto Loader** (`cloudFiles`) with input format `parquet` and source-specific checkpoint.
5. Add technical ingestion columns (timestamp, source file path, ingestion run identifier if useful).
6. Append to Delta; no deduplication, SCD or business cleanup in Bronze.
7. Rerun without new files: target row count must stay unchanged because of the checkpoint.
8. Record results in `LEARNING_NOTES.md` and commit the notebook when done.

## Your first design questions

- How will you select only Snapshot files in a **flat shared entity folder**?
- Where will your checkpoint live? Why must CDC use a different checkpoint?
- What is the difference between source `event_ts` and Bronze `_ingest_ts`?
- What happens if source files arrive out of chronological order?
- What is the safe way to reset the exercise without deleting other pipelines?

## Implementation entry point

`src/notebooks/10_bronze_member_snapshot.py` contains TODOs only. Write the pipeline.

## Validate

Run `sql/01_member_snapshot_checks.sql`; check raw row counts against the generator manifest for B0001. Check the count again after a rerun.

## Completion criteria

A reproducible Auto Loader job or notebook runs on Databricks, records audit metadata, appends without dropping source rows, and does not duplicate unchanged files on rerun.
