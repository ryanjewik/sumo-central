from pyspark.sql import SparkSession
import os
import sys

jdbc = os.environ.get('JDBC_URL', 'jdbc:postgresql://sumo-db:5432/sumo')
user = os.environ.get('DB_USERNAME', 'postgres')
pwd = os.environ.get('DB_PASSWORD', '')

print('Using JDBC URL:', jdbc)

spark = SparkSession.builder.appName('smoke_pg').getOrCreate()
try:
    df = (
        spark.read.format('jdbc')
        .option('url', jdbc)
        .option('dbtable', '(SELECT 1 as test) AS t')
        .option('user', user)
        .option('password', pwd)
        .load()
    )
    df.show()
    print('SUCCESS: read via JDBC')
except Exception as e:
    print('FAILED to read via JDBC:', e)
    raise
finally:
    spark.stop()
