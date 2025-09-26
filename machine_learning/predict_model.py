from pyspark.sql import SparkSession, functions as F
from dotenv import load_dotenv
import os

load_dotenv()

spark = (SparkSession.builder.appName("sumo-build-training-set")
         .getOrCreate())

matches = (spark.read.parquet("s3a://sumo-bucket/api-calls/rikishi_matches/"))

print(matches)
