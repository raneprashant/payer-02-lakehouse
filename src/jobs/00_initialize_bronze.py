import argparse
from pyspark.sql import SparkSession
from payer_lakehouse.bronze.table_initializer import run

parser = argparse.ArgumentParser()
parser.add_argument("--target", required=True, choices=("dev", "test", "prod"))
parser.add_argument("--catalog", required=True, choices=(
    "payer_02_dev", "payer_02_test", "payer_02_prod"))
parser.add_argument("--mode", required=True, choices=("dry-run", "apply"))
args = parser.parse_args()

if args.catalog != f"payer_02_{args.target}":
    raise ValueError("--target and --catalog do not match")

spark = SparkSession.builder.getOrCreate()
run(spark, args.catalog, apply=(args.mode == "apply"))
