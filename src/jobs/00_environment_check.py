import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--target", required=True)
parser.add_argument("--catalog", required=True)
args = parser.parse_args()

allowed = {
    "dev": "payer_02_dev",
    "test": "payer_02_test",
    "prod": "payer_02_prod",
}

if allowed.get(args.target) != args.catalog:
    raise ValueError("Target/catalog mismatch")

spark = SparkSession.builder.getOrCreate()

spark.sql(
    f"SHOW SCHEMAS IN `{args.catalog}`"
).show(truncate=False)

print("Environment check passed!")