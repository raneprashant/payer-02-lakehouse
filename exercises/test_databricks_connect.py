from databricks.connect import DatabricksSession

spark = ( DatabricksSession.builder
            .serverless()
            .profile("DATABRICKS_FREE")
            .getOrCreate()
)

print("Spark session created successfully!", spark.range(5).count())
