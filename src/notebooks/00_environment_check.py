# Databricks notebook source
# Read-only smoke check for bundle target-to-catalog mapping.

# COMMAND ----------
dbutils.widgets.text("target", "dev")
dbutils.widgets.text("catalog", "payer_02_dev")
target = dbutils.widgets.get("target")
catalog = dbutils.widgets.get("catalog")
allowed = {
    "dev": "payer_02_dev",
    "test": "payer_02_test",
    "prod": "payer_02_prod",
}
if allowed.get(target) != catalog:
    raise ValueError(f"Target/catalog mismatch: {target=} {catalog=}")
print(f"Target={target}, expected catalog={catalog}")

# COMMAND ----------
# Read-only metadata query. No tables or volumes are created by this job.
spark.sql(f"SHOW SCHEMAS IN `{catalog}`").show(truncate=False)
print("Environment check passed. Ready for learning exercises.")
