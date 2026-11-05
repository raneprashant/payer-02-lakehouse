# Approved architecture and delivery conventions

## Git repositories

- `payer-02-data-generator`: independent source-data creation, SQLite state, Snapshot/CDC scenario generation.
- `payer-02-lakehouse`: this repository; reads generated input only and implements Bronze, Silver, Gold and CI/CD.

## Unity Catalog layout

| Bundle target | Catalog | Responsibility |
|---|---|---|
| dev | `payer_02_dev` | hands-on development, debugging, full MEDIUM data |
| test | `payer_02_test` | future CI/CD promotion/regression |
| prod | `payer_02_prod` | future CI/CD promotion/smoke only |

Same physical Free Edition workspace; `DATABRICKS_FREE` profile for local CLI. The bundle supplies target-specific catalogs.

Within each catalog: `landing`, `bronze`, `silver`, `gold`, `control`. In DEV:
- Input volume: `payer_02_dev.landing.raw_files`
- Checkpoint volume: `payer_02_dev.control.checkpoints`

## Landing and Bronze

- Files live in flat per-entity folders, e.g. `landing/member_master/`.
- Filenames contain type, batch, arrival date: `member_master_snapshot_B0001_2026-11-01.parquet`.
- Separate append-only Bronze tables for Snapshot and CDC, e.g. `bronze.member_master_snapshot`, `bronze.member_master_cdc`.
- Event-only entities have their own events tables.
- Preserved source values + technical ingestion metadata. No business transformation in Bronze.
- Snapshot and CDC use separate checkpoints and file patterns, even if in one landing directory.
- Source event date, file arrival date, business effective date are distinct and must not be conflated.
- Source CDC sequence, not arrival date or Auto Loader discovery order, is authoritative for replay.

## Silver and Gold (not implemented yet)

- Source key consistency, data quality, deduplication/retraction, CDC reconstruction, SCD Type 1 and 2, effective-dated temporal joins.
- One Coverage active for member on a given business date; each Coverage has uninterrupted, non-overlapping Enrollment segments.
- `enrollment_status` intentionally absent. Address-only changes never create Enrollment by themselves.
- Member > Subscriber/Subscription > Coverage > Enrollment; Practitioner > Role > exact Supplier Location; Address Master and effective-dated Entity-Address Bridge.
- Financial reporting separates claim closure from actual payment, with adjustments and recoveries.
- Future Gold: dimensional model, claim and member-month facts, PMPM, denial and payment reconciliation.

## Note on local Spark

No Spark runtime required on laptop. Unit-test pure Python helpers locally; execute PySpark notebook tasks on Databricks compute.
