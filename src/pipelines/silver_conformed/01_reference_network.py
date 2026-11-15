
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from payer_lakehouse.silver.cdc_helpers import (
    cdc_sequence,
    prepare_cdc,
)
from payer_lakehouse.silver.entity_config import (
    ENTITY_BUSINESS_COLUMNS,
    get_entity_config,
)

# ============================================================
# PAYER 360 - SILVER CONFORMED
# DOMAIN 01: REFERENCE & NETWORK
#
# Entities:
#   1. reference_codes
#   2. specialty
#   3. network
#
# B0001 Snapshot hydration + incremental CDC
# Soft deletes retained
# ============================================================

spark = SparkSession.builder.getOrCreate()

CATALOG = spark.conf.get("source_catalog")

DOMAIN_ENTITIES = [
    "reference_codes",
    "specialty",
    "network",
]


# ------------------------------------------------------------
# Common business-column projection
# ------------------------------------------------------------

def project_entity(df, entity_name):

    columns = ENTITY_BUSINESS_COLUMNS[entity_name]

    business_columns = []

    for column_name, data_type in columns.items():

        value = F.col(column_name).cast(data_type)

        if data_type == "string":
            value = F.trim(value)

        business_columns.append(
            value.alias(column_name)
        )

    return df.select(
        *business_columns,

        # Source lineage
        F.col("source_system").cast("string").alias(
            "_source_system"
        ),
        F.col("batch_id").cast("string").alias(
            "_source_batch_id"
        ),

        # CDC sequencing
        F.col("_source_sequence").cast("long"),
        F.col("_source_record_version").cast("int"),

        # Soft delete
        F.col("is_deleted").cast("boolean"),
        F.col("deleted_at").cast("timestamp"),

        # Audit
        F.col("_silver_updated_at").cast("timestamp"),
    )


# ------------------------------------------------------------
# Register one source-conformed entity
# ------------------------------------------------------------

def register_entity(entity_name):

    config = get_entity_config(entity_name)

    SNAPSHOT_TABLE = (
        f"{CATALOG}.bronze.{entity_name}_snapshot"
    )

    CDC_TABLE = (
        f"{CATALOG}.bronze.{entity_name}_cdc"
    )

    TARGET = (
        f"{CATALOG}.silver.{entity_name}"
    )

    snapshot_view = f"{entity_name}_snapshot_source"
    cdc_view = f"{entity_name}_cdc_source"

    # --------------------------------------------------------
    # Silver Streaming Table
    # --------------------------------------------------------

    dp.create_streaming_table(
        name=TARGET,
        comment=(
            f"Silver Conformed Reference & Network: "
            f"{entity_name}"
        ),
        table_properties={
            "quality": "silver"
        },
    )

    # --------------------------------------------------------
    # Snapshot source - B0001 baseline
    # --------------------------------------------------------

    @dp.temporary_view(name=snapshot_view)
    def snapshot_source():

        bronze = (
            spark.readStream
            .table(SNAPSHOT_TABLE)
            .where(
                F.col("batch_id") == "B0001"
            )
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
                F.col(
                    "snapshot_high_watermark"
                ).cast("long")
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

        return project_entity(
            prepared,
            entity_name
        )

    # --------------------------------------------------------
    # One-time snapshot hydration
    # --------------------------------------------------------

    dp.create_auto_cdc_flow(
        target=TARGET,
        source=snapshot_view,
        keys=config["keys"],
        sequence_by=cdc_sequence(),
        stored_as_scd_type=config["scd_type"],
        track_history_column_list=(
            config["track_history_column_list"]
            if config["scd_type"] == 2
            else None
        ),
        once=True,
        name=f"{entity_name}_snapshot_flow",
    )

    # --------------------------------------------------------
    # Incremental CDC source
    # --------------------------------------------------------

    @dp.temporary_view(name=cdc_view)
    def cdc_source():

        bronze = spark.readStream.table(
            CDC_TABLE
        )

        prepared = prepare_cdc(bronze)

        return project_entity(
            prepared,
            entity_name
        )

    # --------------------------------------------------------
    # Continuous incremental CDC flow
    # --------------------------------------------------------

    dp.create_auto_cdc_flow(
        target=TARGET,
        source=cdc_view,
        keys=config["keys"],
        sequence_by=cdc_sequence(),
        stored_as_scd_type=config["scd_type"],
        track_history_column_list=(
            config["track_history_column_list"]
            if config["scd_type"] == 2
            else None
            ),
        name=f"{entity_name}_cdc_flow",
    )


# ============================================================
# Register all Reference & Network entities
# ============================================================

for entity in DOMAIN_ENTITIES:
    register_entity(entity)
