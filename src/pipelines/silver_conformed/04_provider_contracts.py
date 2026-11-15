
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
# DOMAIN 04: PROVIDER & CONTRACTS
#
# 8 entities
# B0001 Snapshot + B0002 onwards CDC
# Config-driven SCD1 / SCD2
# Soft delete supported
# ============================================================

spark = SparkSession.builder.getOrCreate()

CATALOG = spark.conf.get("source_catalog")

DOMAIN_ENTITIES = [
    "supplier",
    "supplier_location",
    "practitioner",
    "practitioner_specialty",
    "practitioner_role",
    "network_participation",
    "provider_contract",
    "provider_contract_rate",
]


# ============================================================
# 1. Business-column projection
# ============================================================

def project_entity(df, entity_name):

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

    return df.select(
        *business_columns,

        # Source lineage
        F.col("source_system").cast("string").alias(
            "_source_system"
        ),
        F.col("batch_id").cast("string").alias(
            "_source_batch_id"
        ),

        # CDC ordering
        F.col("_source_sequence").cast("long"),
        F.col("_source_record_version").cast("int"),

        # Soft delete
        F.col("is_deleted").cast("boolean"),
        F.col("deleted_at").cast("timestamp"),

        # Silver audit
        F.col("_silver_updated_at").cast("timestamp"),
    )


# ============================================================
# 2. Register each Provider entity
# ============================================================

def register_entity(entity_name):

    config = get_entity_config(entity_name)

    snapshot_table = (
        f"{CATALOG}.bronze.{entity_name}_snapshot"
    )

    cdc_table = (
        f"{CATALOG}.bronze.{entity_name}_cdc"
    )

    TARGET = (
        f"{CATALOG}.silver.{entity_name}"
    )

    snapshot_view = f"{entity_name}_snapshot_source"
    cdc_view = f"{entity_name}_cdc_source"

    scd_type = config["scd_type"]

    # --------------------------------------------------------
    # Silver target
    # --------------------------------------------------------

    dp.create_streaming_table(
        name=TARGET,
        comment=(
            f"Provider & Contracts - {entity_name} "
            f"- SCD Type {scd_type}"
        ),
        table_properties={
            "quality": "silver"
        },
    )

    # --------------------------------------------------------
    # B0001 Snapshot source
    #
    # Preserve the readStream pattern proven in our
    # existing Silver pipelines.
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

        return project_entity(
            prepared,
            entity_name
        )

    # --------------------------------------------------------
    # One-time Snapshot AUTO CDC
    # --------------------------------------------------------

    cdc_options = {
        "target": TARGET,
        "source": snapshot_view,
        "keys": config["keys"],
        "sequence_by": cdc_sequence(),
        "stored_as_scd_type": scd_type,
        "once": True,
        "name": f"{entity_name}_snapshot_flow",
    }

    if scd_type == 2:
        cdc_options["track_history_column_list"] = (
            config["track_history_column_list"]
        )

    dp.create_auto_cdc_flow(**cdc_options)

    # --------------------------------------------------------
    # Incremental CDC source
    # --------------------------------------------------------

    @dp.temporary_view(name=cdc_view)
    def cdc_source():

        bronze = spark.readStream.table(cdc_table)

        prepared = prepare_cdc(bronze)

        return project_entity(
            prepared,
            entity_name
        )

    # --------------------------------------------------------
    # Incremental CDC AUTO CDC
    # --------------------------------------------------------

    cdc_options = {
        "target": TARGET,
        "source": cdc_view,
        "keys": config["keys"],
        "sequence_by": cdc_sequence(),
        "stored_as_scd_type": scd_type,
        "name": f"{entity_name}_cdc_flow",
    }

    if scd_type == 2:
        cdc_options["track_history_column_list"] = (
            config["track_history_column_list"]
        )

    dp.create_auto_cdc_flow(**cdc_options)


# ============================================================
# 3. Register all 8 conformed entities
# ============================================================

for entity in DOMAIN_ENTITIES:
    register_entity(entity)
