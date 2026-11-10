from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery


def parquet_reader(spark: SparkSession, source_path: str, schema_path:str, file_pattern:str) -> DataFrame:
    df =(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("pathGlobFilter", file_pattern)
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(source_path)
    )
    return df


def json_reader(spark: SparkSession, source_path: str, schema_path:str, file_pattern:str) -> DataFrame:
    df =(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", file_pattern)
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(source_path)
    )
    return df


def csv_reader(spark: SparkSession, source_path: str, schema_path:str, file_pattern:str) -> DataFrame:
    df =(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("pathGlobFilter", file_pattern)
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(source_path)
    )
    return df

def add_audit_columns(df: DataFrame, run_id: str) -> DataFrame:
    df = df.withColumn("_ingest_ts", F.current_timestamp()) \
        .withColumn("_source_file", F.col("_metadata.file_path")) \
        .withColumn("_ingest_run_id", F.lit(run_id)) \
        .withColumn("_batch_id", F.regexp_extract(F.col("_metadata.file_name"), r"_(B\d{4})_", 1)) \
        .withColumn("_ingest_date", F.current_date())
    return df

def write_bronze_data(df: DataFrame, target_table: str, checkpoint_path: str) -> StreamingQuery:
    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    return query

