
# ============================================================
# PAYER 360 - SILVER DERIVED
# ELIGIBILITY CHANGE AUDIT
#
# B0001 Snapshot + B0002 onwards CDC
# Coverage + Enrollment
#
# One audit record per unique CDC event.
# Preserves existing records and backfills new metadata.
# ============================================================

import argparse
import uuid

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window, functions as F


parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()

CATALOG = args.catalog
TARGET = f"{CATALOG}.silver.eligibility_change_audit"
RUN_ID = str(uuid.uuid4())


# ============================================================
# 1. Business columns to preserve as JSON
# ============================================================

BUSINESS_FIELDS = {

    "coverage": [
        "coverage_id",
        "member_id",
        "subscription_id",
        "benefit_plan_id",
        "coverage_start_date",
        "coverage_end_date",
        "coverage_status",
        "termination_reason_code",
        "coverage_revision",
    ],

    "enrollment": [
        "enrollment_id",
        "coverage_id",
        "member_id",
        "coverage_start_date",
        "coverage_end_date",
        "coverage_revision",
        "enrollment_start_date",
        "enrollment_end_date",
        "tier_code",
        "pcp_practitioner_role_id",
        "change_reason_code",
    ],
}


# ============================================================
# 2. Additional audit metadata
# ============================================================

EXTRA_COLUMNS = {
    "member_id": "STRING",
    "source_entity": "STRING",
    "operation": "STRING",
    "source_sequence": "BIGINT",
    "source_event_ts": "TIMESTAMP",
    "source_batch_id": "STRING",
}


# ============================================================
# 3. Create audit table if missing
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    change_audit_id STRING,
    coverage_id STRING,
    enrollment_id STRING,
    source_revision INT,
    change_reason STRING,
    old_record_json STRING,
    new_record_json STRING,
    change_processed_at TIMESTAMP,
    run_id STRING,

    member_id STRING,
    source_entity STRING,
    operation STRING,
    source_sequence BIGINT,
    source_event_ts TIMESTAMP,
    source_batch_id STRING
) USING DELTA
""")


# ============================================================
# 4. Extend existing table without losing history
# ============================================================

existing_columns = set(spark.table(TARGET).columns)

for column_name, data_type in EXTRA_COLUMNS.items():

    if column_name not in existing_columns:

        spark.sql(f"""
            ALTER TABLE {TARGET}
            ADD COLUMNS ({column_name} {data_type})
        """)


# ============================================================
# 5. Build history from Snapshot + CDC
# ============================================================

def build_audit(entity):

    business_key = (
        "coverage_id"
        if entity == "coverage"
        else "enrollment_id"
    )

    reason_field = (
        "termination_reason_code"
        if entity == "coverage"
        else "change_reason_code"
    )

    # --------------------------------------------------------
    # Common projection for Snapshot and CDC
    # --------------------------------------------------------

    def project(df, is_snapshot):

        return df.select(

            F.col(business_key)
             .cast("string")
             .alias("_business_key"),

            F.col("coverage_id")
             .cast("string")
             .alias("coverage_id"),

            F.col("member_id")
             .cast("string")
             .alias("member_id"),

            (
                F.lit(None).cast("string")
                if entity == "coverage"
                else F.col("enrollment_id").cast("string")
            ).alias("enrollment_id"),

            (
                F.lit(None).cast("string")
                if is_snapshot
                else F.col("cdc_event_id").cast("string")
            ).alias("_event_id"),

            (
                F.lit("SNAPSHOT")
                if is_snapshot
                else F.upper(F.trim(F.col("operation")))
            ).alias("operation"),

            (
                F.lit(0).cast("int")
                if is_snapshot
                else F.col("source_record_version").cast("int")
            ).alias("_revision"),

            (
                F.col("snapshot_high_watermark").cast("long")
                if is_snapshot
                else F.col("source_sequence").cast("long")
            ).alias("source_sequence"),

            (
                F.lit(None).cast("timestamp")
                if is_snapshot
                else F.col("event_ts").cast("timestamp")
            ).alias("source_event_ts"),

            F.col("batch_id")
             .cast("string")
             .alias("source_batch_id"),

            F.lit(entity.upper()).alias("source_entity"),

            F.to_json(
                F.struct(
                    *[
                        F.col(c)
                        for c in BUSINESS_FIELDS[entity]
                    ]
                )
            ).alias("_record_json"),

            (
                F.lit(None).cast("string")
                if is_snapshot
                else F.col(reason_field).cast("string")
            ).alias("_source_reason"),
        )

    # --------------------------------------------------------
    # B0001 baseline
    # --------------------------------------------------------

    snapshot_df = (
        spark.table(
            f"{CATALOG}.bronze.{entity}_snapshot"
        )
        .filter(F.col("batch_id") == "B0001")
        .dropDuplicates([business_key])
    )

    seed = project(snapshot_df, is_snapshot=True)

    # --------------------------------------------------------
    # All later CDC events
    # --------------------------------------------------------

    cdc_df = (
        spark.table(
            f"{CATALOG}.bronze.{entity}_cdc"
        )
        .filter(F.col("cdc_event_id").isNotNull())
        .filter(
            F.upper(F.trim(F.col("operation")))
             .isin("I", "U", "D")
        )
        .dropDuplicates(["cdc_event_id"])
    )

    events = project(cdc_df, is_snapshot=False)

    # --------------------------------------------------------
    # Reconstruct changes per business record
    #
    # Source revision orders changes for the same key.
    # Source sequence breaks ordering ties.
    # --------------------------------------------------------

    version_window = (
        Window
        .partitionBy("_business_key")
        .orderBy(
            "_revision",
            "source_sequence",
            "_event_id"
        )
    )

    history = (
        seed.unionByName(events)

        .withColumn(
            "_state_json",
            F.when(
                F.col("operation") == "D",
                F.lit(None).cast("string")
            ).otherwise(
                F.col("_record_json")
            )
        )

        .withColumn(
            "_previous_json",
            F.lag("_state_json").over(version_window)
        )

        # Snapshot establishes the original state.
        # Only CDC events become audit records.
        .filter(F.col("_event_id").isNotNull())
    )

    # --------------------------------------------------------
    # Map into approved Silver audit contract
    # --------------------------------------------------------

    return history.select(

        # Same deterministic ID as original implementation.
        F.sha2(
            F.concat_ws(
                "|",
                F.lit(entity),
                F.col("_event_id")
            ),
            256
        ).alias("change_audit_id"),

        "coverage_id",
        "enrollment_id",

        F.col("_revision").alias("source_revision"),

        F.concat_ws(
            " | ",
            F.concat(
                F.lit(entity.upper() + "_"),
                F.col("operation")
            ),
            F.when(
                F.length(
                    F.trim(F.col("_source_reason"))
                ) > 0,
                F.col("_source_reason")
            )
        ).alias("change_reason"),

        F.col("_previous_json").alias("old_record_json"),
        F.col("_state_json").alias("new_record_json"),

        F.current_timestamp().alias("change_processed_at"),
        F.lit(RUN_ID).alias("run_id"),

        # New audit metadata
        "member_id",
        "source_entity",
        "operation",
        "source_sequence",
        "source_event_ts",
        "source_batch_id",
    )


# ============================================================
# 6. Combine Coverage and Enrollment audit events
# ============================================================

updates = (
    build_audit("coverage")
    .unionByName(build_audit("enrollment"))
)


# ============================================================
# 7. Idempotent Delta MERGE
#
# Existing rows:
#   Backfill only the new metadata.
#   Preserve original before/after JSON and audit timestamp.
#
# New rows:
#   Insert full audit record.
# ============================================================

metadata_changed = " OR ".join(
    f"NOT (t.{field} <=> s.{field})"
    for field in EXTRA_COLUMNS
)

(
    DeltaTable.forName(spark, TARGET)
    .alias("t")
    .merge(
        updates.alias("s"),
        "t.change_audit_id = s.change_audit_id"
    )
    .whenMatchedUpdate(
        condition=metadata_changed,
        set={
            field: f"s.{field}"
            for field in EXTRA_COLUMNS
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)


print(
    f"Eligibility change audit completed: {TARGET}; "
    f"run_id={RUN_ID}"
)
