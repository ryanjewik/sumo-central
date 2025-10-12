from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("spark_smoke") \
    .getOrCreate()

print("✅ Spark started")
print("Count:", spark.range(0, 10).count())

spark.stop()
