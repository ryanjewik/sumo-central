# /opt/airflow/dags/jobs/spark_smoke.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("spark_smoke").getOrCreate()
df = spark.range(0, 10)
total = df.count()
print("✅ Spark local test count =", total)
spark.stop()
