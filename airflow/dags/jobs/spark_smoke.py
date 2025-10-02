from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("spark_smoke") \
    .master("local[*]") \
    .getOrCreate()

print("✅ Spark started")
print("Count:", spark.range(0, 10).count())

spark.stop()
