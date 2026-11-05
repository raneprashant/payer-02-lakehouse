# Exercise 02 — Bronze Member Snapshot → Silver Member

Start only after Exercise 01 is complete.

**Input:** `payer_02_dev.bronze.member_master_snapshot`

**Target:** `payer_02_dev.silver.member` (proposed current-state grain: one row per member_id)

## Design before code

- Specify target types and nullability from the source contract.
- Identify required fields and define quarantine/error behavior.
- Explain how duplicate records will be handled if encountered (test explicitly).
- State whether status and demographic changes will be SCD Type 1 or Type 2; **start with initial Snapshot only**.
- Define reconciliation counts: source, invalid, accepted, target.

Implement `src/notebooks/20_silver_member.py` only after approval of your mapping plan. Validate via `sql/02_member_silver_checks.sql`.

**Later:** We will add Member CDC in a different exercise, not mix it into this first Snapshot load.
