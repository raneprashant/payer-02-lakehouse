-- EXERCISE 01: Run AFTER you've implemented Bronze Member Snapshot.
-- These queries intentionally provide EXPECTATIONS, not transformation code.
SELECT COUNT(*) AS bronze_member_snapshot_rows
FROM payer_02_dev.bronze.member_master_snapshot;

SELECT member_id, COUNT(*) AS records_per_member
FROM payer_02_dev.bronze.member_master_snapshot
GROUP BY member_id
HAVING COUNT(*) > 1;

-- Confirm ingestion audit columns exist and are populated.
-- Uncomment after you implement _source_file and _ingest_ts:
-- SELECT COUNT(*) AS missing_audit
-- FROM payer_02_dev.bronze.member_master_snapshot
-- WHERE _source_file IS NULL OR _ingest_ts IS NULL;

-- Record results before and after rerunning the same Auto Loader code.
-- With an unchanged checkpoint and the same source files, rows must not double.
