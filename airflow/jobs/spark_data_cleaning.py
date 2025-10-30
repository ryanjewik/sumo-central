from __future__ import annotations

import argparse
import logging
import os
from typing import Optional

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")



def create_spark_session(
    app_name: str = "data_cleaning_job",
    driver_memory: Optional[str] = "4g",
    executor_memory: Optional[str] = "4g",
) -> SparkSession:
    driver_mem = driver_memory or os.environ.get("SPARK_DRIVER_MEMORY")
    executor_mem = executor_memory or os.environ.get("SPARK_EXECUTOR_MEMORY")

    builder = (
        SparkSession.builder.appName(app_name)
        # we assume the image already has hadoop-aws + aws-java-sdk
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    )

    # optional: let container env decide region, not this script
    # if you really want to force:
    # builder = builder.config("spark.hadoop.fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")

    if driver_mem:
        LOG.info("Setting spark.driver.memory=%s", driver_mem)
        builder = builder.config("spark.driver.memory", driver_mem)
    if executor_mem:
        LOG.info("Setting spark.executor.memory=%s", executor_mem)
        builder = builder.config("spark.executor.memory", executor_mem)

    spark = builder.getOrCreate()

    # Hadoop-level S3A config (lightweight)
    hadoop_conf = spark._jsc.hadoopConfiguration()

    aws_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_token = os.environ.get("AWS_SESSION_TOKEN")

    # only set creds here if present; otherwise let the provider chain (instance role, etc.) handle it
    if aws_key and aws_secret:
        LOG.info("Applying AWS credentials to Hadoop config from environment")
        hadoop_conf.set("fs.s3a.access.key", aws_key)
        hadoop_conf.set("fs.s3a.secret.key", aws_secret)
        if aws_token:
            hadoop_conf.set("fs.s3a.session.token", aws_token)

    # good defaults, but not overkill
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.fast.upload", "true")

    return spark


def process_data(
	input_path: str,
	output_path: str,
	app_name: str = "data_cleaning_job",
	driver_memory: Optional[str] = None,
	executor_memory: Optional[str] = None,
) -> None:
	LOG.info("Starting process_data: %s -> %s", input_path, output_path)
	spark = create_spark_session(app_name=app_name, driver_memory=driver_memory, executor_memory=executor_memory)

	try:
		print("starting process")
		# Read primary matches input (kept as hardcoded internal path)
		df = (
			spark.read
			.option("multiLine", "true")                # set to "false" if you have JSON Lines
			.option("mode", "PERMISSIVE")               # or "FAILFAST"
			.option("recursiveFileLookup", "true")
			.option("pathGlobFilter", "*.json")         # only JSON files
			.json("s3a://ryans-sumo-bucket/sumo-api-calls/rikishi_matches/")
		)
		print("read matches")
		rows = df.select(explode(col("records")).alias("match"))
		flat = rows.select("match.*")   # expand struct into columns
		silver = (flat
			.dropDuplicates()
		) 
		silver = (silver
			.withColumn("year", F.col("bashoId").cast("string").substr(1,4))
			.withColumn("month", F.col("bashoId").cast("string").substr(5,2))
			.withColumn("match_date",
						F.to_date(
							F.concat_ws("-", F.col("year"), F.col("month"), F.lpad(F.col("day"), 2, "0")),
							"yyyy-MM-dd"
						)
			)
			.withColumn(
				"match_id",
				F.concat_ws("", F.date_format(F.col("match_date"), "yyyyMMdd"), F.col("matchNo").cast("string")).cast("long")
			)
			.drop("year", "month")
		)
		(silver.write
			.mode("overwrite")
			.option("compression", "snappy")
			.partitionBy("bashoId")                        # or by year/month/day
			.parquet("s3a://ryans-sumo-bucket/silver/rikishi_matches/"))
		print("wrote matches")
		df = (spark.read
			.option("multiLine", "true")
			.option("mode", "PERMISSIVE")
			.option("recursiveFileLookup", "true")
			.option("pathGlobFilter", "*.json")
			.json("s3a://ryans-sumo-bucket/sumo-api-calls/rikishis/")
		)
		print("read rikishis")
		(df.write
			.mode("overwrite")
			.option("compression", "snappy")
			.parquet("s3a://ryans-sumo-bucket/silver/rikishis/")
		)
		print("wrote rikishis")
		df = (spark.read
			.option("multiLine", "true")
			.option("mode", "PERMISSIVE")
			.option("recursiveFileLookup", "true")
			.option("pathGlobFilter", "*.json")
			.json("s3a://ryans-sumo-bucket/sumo-api-calls/rikishi_stats/")
		)
		print("read rikishi stats")
		df = df.withColumn(
			"rikishi_id",
			F.regexp_extract(F.input_file_name(), r"rikishi_(\d+)\.json", 1)
		)

		(df.write
			.mode("overwrite")
			.option("compression", "snappy")
			.parquet("s3a://ryans-sumo-bucket/silver/rikishi_stats/")
		)
		print("wrote rikishi stats")
		df = (spark.read
			.option("multiLine", "true")
			.option("mode", "PERMISSIVE")
			.option("recursiveFileLookup", "true")
			.option("pathGlobFilter", "*.json")
			.json("s3a://ryans-sumo-bucket/sumo-api-calls/basho/")
		)
		print("read bashos")
		(df.write
			.mode("overwrite")
			.option("compression", "snappy")
			.partitionBy("location")
			.parquet("s3a://ryans-sumo-bucket/silver/bashos/")
		)
		print("wrote bashos")
		#silver writes completed
	except Exception as e:
		LOG.error("Error during process_data: %s", e)
		print("error during process_data    %s" % e)
		raise

	finally:
		spark.stop()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser("Simple Spark data cleaning job")
    p.add_argument("--input", default="s3a://ryans-sumo-bucket/sumo-api-calls/rikishi_matches/")
    p.add_argument("--output", default="s3a://ryans-sumo-bucket/silver/rikishi_matches/")
    p.add_argument("--app-name", default="data_cleaning_job")
    p.add_argument("--driver-memory")
    p.add_argument("--executor-memory")
    return p.parse_args()

if __name__ == "__main__":
    args = _parse_args()
    process_data(
        input_path=args.input,
        output_path=args.output,
        app_name=args.app_name,
        driver_memory=args.driver_memory,
        executor_memory=args.executor_memory,
    )




