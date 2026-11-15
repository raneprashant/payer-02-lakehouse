
# ============================================================
# PAYER 360 - SILVER CONFORMED
# Reusable CDC Helpers
# ============================================================

from pyspark.sql import functions as F


def prepare_cdc(df):
    """
    Prepare a Bronze CDC DataFrame for Silver processing.

    Supports:
    - Duplicate event elimination
    - I / U / D operations
    - Source sequencing metadata
    - Soft deletes
    """

    # 1. Deduplicate CDC events
    df = df.dropDuplicates(["cdc_event_id"])

    # 2. Normalize CDC operation
    operation = F.upper(F.trim(F.col("operation")))

    # 3. Apply common CDC transformations
    return (
        df
        .filter(operation.isin("I", "U", "D"))

        .withColumn(
            "_source_sequence",
            F.col("source_sequence").cast("long")
        )

        .withColumn(
            "_source_record_version",
            F.col("source_record_version").cast("int")
        )

        .withColumn(
            "is_deleted",
            operation == F.lit("D")
        )

        .withColumn(
            "deleted_at",
            F.when(
                operation == F.lit("D"),
                F.col("event_ts").cast("timestamp")
            ).otherwise(
                F.lit(None).cast("timestamp")
            )
        )

        .withColumn(
            "_silver_updated_at",
            F.current_timestamp()
        )
    )


def cdc_sequence():
    """
    Business record version first.
    Source sequence breaks ties.
    """

    return F.struct(
        F.col("_source_record_version"),
        F.col("_source_sequence")
    )