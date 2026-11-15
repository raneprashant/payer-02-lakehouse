
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

from payer_lakehouse.silver.cdc_helpers import (
    prepare_cdc,
    cdc_sequence,
)

from payer_lakehouse.silver.entity_config import (
    get_entity_config,
    ENTITY_BUSINESS_COLUMNS,
)

# ============================================================
# PAYER 360 - SILVER CONFORMED
# DOMAIN 06: CLAIMS & FINANCE
#
# Claim Header + Claim Line:
#   B0001 Snapshot + B0002 onwards CDC
#   AUTO CDC SCD1, soft deletes
#
# Claim Status, Adjustment, Payment Remittance:
#   Event-only streaming append + deduplication
#
# All definitions are config-driven.
# ============================================================

spark = SparkSession.builder.getOrCreate()

CATALOG = spark.conf.get("source_catalog")

CDC_ENTITIES = [
    "claim_header",
    "claim_line",
]

EVENT_ENTITIES = [
    "claim_status_event",
    "claim_adjustment",
    "payment_remittance",
]


# ============================================================
# 1. Common business-column projection
# ============================================================

def business_projection(df, entity_name):

    columns = ENTITY_BUSINESS_COLUMNS[entity_name]

    business_columns = []

    for column_name, data_type in columns.items():

        value = F.col(column_name)

        if data_type == "date":
            value = F.to_date(value)

        elif data_type == "string":
            value = F.trim(value.cast("string"))

        else:
            value = value.cast(data_type)

        business_columns.append(
            value.alias(column_name)
        )

    return business_columns


# ============================================================
# 2. Snapshot + CDC projection for Claim entities
# ============================================================

def project_claim(df, entity_name):

    return df.select(
        *business_projection(df, entity_name),

        # Source lineage
        F.col("source_system").cast("string").alias(
            "_source_system"
        ),
        F.col("batch_id").cast("string").alias(
            "_source_batch_id"
        ),

        # Source version / sequencing
        F.col("_source_sequence").cast("long"),
        F.col("_source_record_version").cast("int"),

        # Source retraction (soft delete)
        F.col("is_deleted").cast("boolean"),
        F.col("deleted_at").cast("timestamp"),

        # Silver audit
        F.col("_silver_updated_at").cast("timestamp"),
    )


# ============================================================
# 3. Register a Snapshot + CDC entity
# ============================================================

def register_claim_entity(entity_name):

    config = get_entity_config(entity_name)

    snapshot_table = (
        f"{CATALOG}.bronze.{entity_name}_snapshot"
    )

    cdc_table = (
        f"{CATALOG}.bronze.{entity_name}_cdc"
    )

    target = f"{CATALOG}.silver.{entity_name}"

    snapshot_view = f"{entity_name}_snapshot_source"
    cdc_view = f"{entity_name}_cdc_source"

    # --------------------------------------------------------
    # Create Silver target
    # --------------------------------------------------------

    dp.create_streaming_table(
        name=target,
        comment=(
            f"Silver Conformed Claims & Finance - "
            f"{entity_name} - SCD{config['scd_type']}"
        ),
        table_properties={
            "quality": "silver",
        },
    )

    # --------------------------------------------------------
    # B0001 Snapshot source
    # Retains our existing working readStream pattern.
    # --------------------------------------------------------

    @dp.temporary_view(name=snapshot_view)
    def snapshot_source():

        bronze = (
            spark.readStream.table(snapshot_table)
            .where(F.col("batch_id") == "B0001")
        )

        prepared = (
            bronze

            .withColumn(
                "_source_record_version",
                F.when(
                    F.col("batch_id") == "B0001",
                    F.lit(0)
                ).otherwise(
                    F.lit(None)
                ).cast("int")
            )

            .withColumn(
                "_source_sequence",
                F.col("snapshot_high_watermark").cast("long")
            )

            .withColumn(
                "is_deleted",
                F.lit(False)
            )

            .withColumn(
                "deleted_at",
                F.lit(None).cast("timestamp")
            )

            .withColumn(
                "_silver_updated_at",
                F.current_timestamp()
            )
        )

        return project_claim(
            prepared,
            entity_name
        )

    # --------------------------------------------------------
    # One-time Snapshot AUTO CDC flow
    # --------------------------------------------------------

    snapshot_options = {
        "target": target,
        "source": snapshot_view,
        "keys": config["keys"],
        "sequence_by": cdc_sequence(),
        "stored_as_scd_type": config["scd_type"],
        "once": True,
        "name": f"{entity_name}_snapshot_flow",
    }

    if config["scd_type"] == 2:
        snapshot_options["track_history_column_list"] = (
            config["track_history_column_list"]
        )

    dp.create_auto_cdc_flow(**snapshot_options)

    # --------------------------------------------------------
    # Incremental CDC source
    # --------------------------------------------------------

    @dp.temporary_view(name=cdc_view)
    def cdc_source():

        bronze = spark.readStream.table(cdc_table)

        prepared = prepare_cdc(bronze)

        return project_claim(
            prepared,
            entity_name
        )

    # --------------------------------------------------------
    # Incremental CDC AUTO CDC flow
    # --------------------------------------------------------

    cdc_options = {
        "target": target,
        "source": cdc_view,
        "keys": config["keys"],
        "sequence_by": cdc_sequence(),
        "stored_as_scd_type": config["scd_type"],
        "name": f"{entity_name}_cdc_flow",
    }

    if config["scd_type"] == 2:
        cdc_options["track_history_column_list"] = (
            config["track_history_column_list"]
        )

    dp.create_auto_cdc_flow(**cdc_options)


# ============================================================
# 4. Register Claim Header and Claim Line
# ============================================================

for entity in CDC_ENTITIES:

    register_claim_entity(entity)


# ============================================================
# 5. Event-only business projection
#
# Event streams are not source CDC I/U/D.
# Every unique event remains as a separate business record.
# ============================================================

def optional_column(df, column_name, data_type):

    if column_name in df.columns:
        return F.col(column_name).cast(data_type)

    return F.lit(None).cast(data_type)


def project_event(df, entity_name):

    config = get_entity_config(entity_name)

    # Events have their own business payload and source metadata.
    batch_column = (
        "batch_id"
        if "batch_id" in df.columns
        else "_batch_id"
    )

    return (
        df.select(
            *business_projection(df, entity_name),

            optional_column(
                df, "source_system", "string"
            ).alias("_source_system"),

            optional_column(
                df, batch_column, "string"
            ).alias("_source_batch_id"),

            optional_column(
                df, "source_sequence", "long"
            ).alias("_source_sequence"),

            optional_column(
                df, "_source_file", "string"
            ).alias("_source_file"),

            F.current_timestamp().alias(
                "_silver_updated_at"
            ),
        )

        # One accepted row per event business key.
        # Retain the streaming deduplication state across runs.
        .dropDuplicates(config["keys"])
    )


# ============================================================
# 6. Claim Status Events
# ============================================================

@dp.table(
    name=f"{CATALOG}.silver.claim_status_event",
    comment="Silver Conformed - Claim Lifecycle Status Events",
    table_properties={
        "quality": "silver",
        "processing_strategy": "APPEND_EVENT",
    },
)
def claim_status_event():

    bronze = spark.readStream.table(
        f"{CATALOG}.bronze.claim_status_event_events"
    )

    return project_event(
        bronze,
        "claim_status_event"
    )


# ============================================================
# 7. Claim Adjustment Events
# ============================================================

@dp.table(
    name=f"{CATALOG}.silver.claim_adjustment",
    comment="Silver Conformed - Claim Adjustment Events",
    table_properties={
        "quality": "silver",
        "processing_strategy": "APPEND_EVENT",
    },
)
def claim_adjustment():

    bronze = spark.readStream.table(
        f"{CATALOG}.bronze.claim_adjustment_events"
    )

    return project_event(
        bronze,
        "claim_adjustment"
    )


# ============================================================
# 8. Payment and Remittance Events
# ============================================================

@dp.table(
    name=f"{CATALOG}.silver.payment_remittance",
    comment="Silver Conformed - Payment, Void and Recovery Events",
    table_properties={
        "quality": "silver",
        "processing_strategy": "APPEND_EVENT",
    },
)
def payment_remittance():

    bronze = spark.readStream.table(
        f"{CATALOG}.bronze.payment_remittance_events"
    )

    return project_event(
        bronze,
        "payment_remittance"
    )
