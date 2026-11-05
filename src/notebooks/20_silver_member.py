# Databricks notebook source
# EXERCISE 02 — Write the initial Member Snapshot => Silver code yourself.
# Input: payer_02_dev.bronze.member_master_snapshot
# Output (proposed): payer_02_dev.silver.member
# Focus: target grain, type/required checks, source-to-Silver mapping, DQ tests.

# COMMAND ----------
dbutils.widgets.text("catalog", "payer_02_dev")
catalog = dbutils.widgets.get("catalog")

# TODO 1: Describe Silver table grain and business key: member_id.
# TODO 2: Read Bronze member snapshot and validate fields.
# TODO 3: Define quality rejection/quarantine behavior explicitly.
# TODO 4: Write the initial Silver table and prove key uniqueness.
# TODO 5: We will add CDC handling in a later exercise; do not improvise it now.

raise NotImplementedError("Exercise 02: implement the initial Silver member pipeline")
