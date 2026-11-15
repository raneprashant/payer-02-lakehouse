
# Add the deployed src directory to Python's import path.
#import sys

#sys.path.insert(0,
#    "/Workspace/Users/ranep2006@gmail.com/.bundle/"
#    "payer_02_lakehouse/dev/files/src",
#)


from payer_lakehouse.silver.cdc_helpers import (
    prepare_cdc,
    cdc_sequence,
)

from payer_lakehouse.silver.entity_config import get_entity_config

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

# ============================================================
# PAYER 360 - SILVER CONFORMED
# Domain : Membership & Eligibility
# Entity : Member Master
#
# B0001 Snapshot + B0002 onwards CDC
# Configurable SCD Type 1 / 2 with Soft Delete
#
# Two AUTO CDC flows -> One Silver target
# ============================================================

spark = SparkSession.builder.getOrCreate()

CATALOG = spark.conf.get("source_catalog")

SNAPSHOT_TABLE = f"{CATALOG}.bronze.member_master_snapshot"
CDC_TABLE = f"{CATALOG}.bronze.member_master_cdc"
TARGET = f"{CATALOG}.silver.member_master"

MEMBER_CONFIG = get_entity_config("member_master")


# ------------------------------------------------------------
# 1. Common Silver transformation
# ------------------------------------------------------------

def project_member(df):

    return df.select(
        F.trim("member_id").alias("member_id"),
        F.trim("first_name").alias("first_name"),
        F.trim("last_name").alias("last_name"),
        F.to_date("birth_date").alias("birth_date"),
        F.upper(F.trim("sex_code")).alias("sex_code"),
        F.upper(F.trim("member_status")).alias("member_status"),
        F.col("household_id").cast("string"),
        F.to_date("death_date").alias("death_date"),
        F.col("phone").cast("string"),
        F.col("email").cast("string"),

        # Source lineage
        F.col("source_system").alias("_source_system"),
        F.col("batch_id").alias("_source_batch_id"),
        F.col("_source_sequence").cast("long"),
        F.col("_source_record_version").cast("int"),

        # Soft delete
        F.col("is_deleted").cast("boolean"),
        F.col("deleted_at").cast("timestamp"),

        # Silver audit
        F.col("_silver_updated_at").cast("timestamp"),
    )


# ------------------------------------------------------------
# 2. Create Silver target
# ------------------------------------------------------------

dp.create_streaming_table(
    name=TARGET,
    comment="Conformed Member Master - SCD2 with Soft Delete",
    table_properties={
        "quality": "silver"
    },
)


# ------------------------------------------------------------
# 3. B0001 Snapshot source - one-time load
# ------------------------------------------------------------

@dp.temporary_view(name="member_master_snapshot_source")
def member_master_snapshot_source():

    bronze = (
        spark.readStream.table(SNAPSHOT_TABLE)
        .where(F.col("batch_id") == "B0001")
    )

    snapshot = (
        bronze
        .withColumn(
            "_source_record_version",
            # Keep the field nullable, matching the CDC sequencing struct.
            # The source filter ensures every baseline row still receives 0.
            F.when(
                F.col("batch_id") == "B0001", F.lit(0)
            ).otherwise(F.lit(None)).cast("int")
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

    return project_member(snapshot)


# ------------------------------------------------------------
# 4. B0001 Snapshot AUTO CDC flow
# ------------------------------------------------------------

dp.create_auto_cdc_flow(
    target=TARGET,
    source="member_master_snapshot_source",
    #keys=["member_id"],
    keys=MEMBER_CONFIG["keys"],

    sequence_by=F.struct(
        F.col("_source_record_version"),
        F.col("_source_sequence")
    ),

    stored_as_scd_type=MEMBER_CONFIG["scd_type"],
    track_history_column_list=(
        MEMBER_CONFIG["track_history_column_list"]
        if MEMBER_CONFIG["scd_type"] == 2
        else None),    
    once=True,
    name="member_master_snapshot_flow",
)


# ------------------------------------------------------------
# 5. CDC source - B0002 onwards
# ------------------------------------------------------------
@dp.temporary_view(name="member_master_cdc_source")
def member_master_cdc_source():

    bronze = spark.readStream.table(CDC_TABLE)

    return project_member(
        prepare_cdc(bronze)
    )

# ------------------------------------------------------------
# 6. Incremental CDC AUTO CDC flow
# ------------------------------------------------------------

dp.create_auto_cdc_flow(
    target=TARGET,
    source="member_master_cdc_source",
    #keys=["member_id"],
    keys=MEMBER_CONFIG["keys"],

    sequence_by=cdc_sequence(),

    stored_as_scd_type=MEMBER_CONFIG["scd_type"],
    track_history_column_list=(
        MEMBER_CONFIG["track_history_column_list"]
        if MEMBER_CONFIG["scd_type"] == 2
        else None),

    name="member_master_cdc_flow",
)


# ============================================================
# REMAINING MEMBERSHIP & ELIGIBILITY ENTITIES
# ============================================================

from payer_lakehouse.silver.entity_config import (
    get_entity_config,
    ENTITY_BUSINESS_COLUMNS,
)

from payer_lakehouse.silver.cdc_helpers import (
    prepare_cdc,
    cdc_sequence,
)


def register_silver_entity(entity_name):
    """
    Register one Silver table and its two AUTO CDC flows.

    B0001 : One-time snapshot
    B0002+: Incremental CDC
    """

    config = get_entity_config(entity_name)
    columns = ENTITY_BUSINESS_COLUMNS[entity_name]

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
    # Common business and technical column projection
    # --------------------------------------------------------

    def project_entity(df):

        business_cols = []

        for col_name, data_type in columns.items():

            column = F.col(col_name)

            if data_type == "date":
                column = F.to_date(column)
            else:
                column = column.cast(data_type)

            business_cols.append(
                column.alias(col_name)
            )

        return df.select(
            *business_cols,

            F.col("source_system").alias("_source_system"),
            F.col("batch_id").alias("_source_batch_id"),

            F.col("_source_sequence").cast("long"),
            F.col("_source_record_version").cast("int"),

            F.col("is_deleted").cast("boolean"),
            F.col("deleted_at").cast("timestamp"),
            F.col("_silver_updated_at").cast("timestamp"),
        )

    # --------------------------------------------------------
    # Create Silver streaming table
    # --------------------------------------------------------

    dp.create_streaming_table(
        name=target,
        comment=f"Silver Conformed - {entity_name}",
        table_properties={"quality": "silver"},
    )

    # --------------------------------------------------------
    # B0001 Snapshot source
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
            .withColumn("is_deleted", F.lit(False))
            .withColumn(
                "deleted_at",
                F.lit(None).cast("timestamp")
            )
            .withColumn(
                "_silver_updated_at",
                F.current_timestamp()
            )
        )

        return project_entity(prepared)

    # --------------------------------------------------------
    # Initial snapshot AUTO CDC flow
    # --------------------------------------------------------

    cdc_options = {
        "target": target,
        "source": snapshot_view,
        "keys": config["keys"],
        "sequence_by": cdc_sequence(),
        "stored_as_scd_type": config["scd_type"],
        "once": True,
        "name":f"{entity_name}_snapshot_flow",
    }

    if config["scd_type"] == 2:
        cdc_options["track_history_column_list"] = (config["track_history_column_list"])

    dp.create_auto_cdc_flow(**cdc_options)

    #dp.create_auto_cdc_flow(
    #    target=target,
    #    source=snapshot_view,
    #    keys=config["keys"],
    #    sequence_by=cdc_sequence(),
    #    stored_as_scd_type=config["scd_type"]
    #    once=True,
    #    name=f"{entity_name}_snapshot_flow",
    #)

    # --------------------------------------------------------
    # Incremental CDC source
    # --------------------------------------------------------

    @dp.temporary_view(name=cdc_view)
    def incremental_cdc_source():

        bronze = spark.readStream.table(cdc_table)

        prepared = prepare_cdc(bronze)

        return project_entity(prepared)

    # --------------------------------------------------------
    # Incremental CDC flow
    # --------------------------------------------------------

    cdc_options = {
        "target": target,
        "source": cdc_view,
        "keys": config["keys"],
        "sequence_by": cdc_sequence(),
        "stored_as_scd_type": config["scd_type"],
        "name":f"{entity_name}_cdc_flow",        
    }

    if config["scd_type"] == 2:
        cdc_options["track_history_column_list"] = (config["track_history_column_list"])

    dp.create_auto_cdc_flow(**cdc_options)


    #dp.create_auto_cdc_flow(
    #    target=target,
    #    source=cdc_view,
    #    keys=config["keys"],
    #    sequence_by=cdc_sequence(),
    #    stored_as_scd_type=config["scd_type"]
    #    track_history_column_list=config.get("track_history_column_list")
    #    name=f"{entity_name}_cdc_flow",
    #)


# ============================================================
# REGISTER DOMAIN ENTITIES
# Member Master already registered earlier in this script.
# ============================================================

register_silver_entity("subscription")
register_silver_entity("subscription_membership")
register_silver_entity("coverage")
register_silver_entity("enrollment")
