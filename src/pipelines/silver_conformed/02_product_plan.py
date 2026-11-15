
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
# Domain: Product & Plan
#
# 6 entities
# B0001 Snapshot + B0002 onwards CDC
# Soft delete + version-first sequencing
# ============================================================

spark = SparkSession.builder.getOrCreate()

CATALOG = spark.conf.get("source_catalog")

DOMAIN_ENTITIES = [
    "product",
    "benefit_plan",
    "employer_group",
    "employer_subgroup",
    "plan_offering",
    "plan_network",
]


# ------------------------------------------------------------
# 1. Common business column projection
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

        F.col("_source_sequence").cast("long"),
        F.col("_source_record_version").cast("int"),

        # Soft delete
        F.col("is_deleted").cast("boolean"),
        F.col("deleted_at").cast("timestamp"),

        # Silver processing audit
        F.col("_silver_updated_at").cast("timestamp"),
    )


# ------------------------------------------------------------
# 2. Register one entity with Snapshot + CDC flows
# ------------------------------------------------------------

def register_entity(entity_name):

    config = get_entity_config(entity_name)

    snapshot_table = (
        f"{CATALOG}.bronze.{entity_name}_snapshot"
    )

    cdc_table = (
        f"{CATALOG}.bronze.{entity_name}_cdc"
    )

    target = (
        f"{CATALOG}.silver.{entity_name}"
    )

    snapshot_view = f"{entity_name}_snapshot_source"
    cdc_view = f"{entity_name}_cdc_source"

    # --------------------------------------------------------
    # Create Silver target
    # --------------------------------------------------------

    dp.create_streaming_table(
        name=target,
        comment=f"Silver Conformed Product & Plan - {entity_name}",
        table_properties={
            "quality": "silver",
        },
    )

    # --------------------------------------------------------
    # B0001 Snapshot Source
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

        return project_entity(prepared, entity_name)

    # --------------------------------------------------------
    # B0001 One-time Snapshot Flow
    # --------------------------------------------------------

    dp.create_auto_cdc_flow(
        target=target,
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
    # Incremental CDC Source
    # --------------------------------------------------------

    @dp.temporary_view(name=cdc_view)
    def cdc_source():

        bronze = spark.readStream.table(cdc_table)

        prepared = prepare_cdc(bronze)

        return project_entity(prepared, entity_name)

    # --------------------------------------------------------
    # Incremental CDC Flow
    # --------------------------------------------------------

    dp.create_auto_cdc_flow(
        target=target,
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
# 3. Register all six Product & Plan entities
# ============================================================

for entity in DOMAIN_ENTITIES:
    register_entity(entity)
