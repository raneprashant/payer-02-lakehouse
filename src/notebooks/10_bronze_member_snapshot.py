# Databricks notebook source
# EXERCISE 01 — Complete yourself; do not deploy this notebook in a production job yet.
# Source: /Volumes/payer_02_dev/landing/raw_files/landing/member_master/
# File pattern: member_master_snapshot_B*.parquet (and a separate CDC pattern)
# Target: payer_02_dev.bronze.member_master_snapshot

# COMMAND ----------
dbutils.widgets.text("catalog", "payer_02_dev")
catalog = dbutils.widgets.get("catalog")

# TODO 1: Derive source, checkpoint and target paths from catalog and entity name.
# TODO 2: Read ONLY member_master_snapshot_*.parquet via Auto Loader.
# TODO 3: Use a defined schema from the approved source contract (not loose inference).
# TODO 4: Add _ingest_ts, _source_file, _ingest_run_id (or documented audit fields).
# TODO 5: Append to Bronze Delta; select availableNow/triggered processing.
# TODO 6: Use its own checkpoint; NEVER share it with member CDC.
# TODO 7: Demonstrate idempotent rerun: unchanged files should not reappend.

raise NotImplementedError("Exercise 01: implement the Bronze member snapshot pipeline")
