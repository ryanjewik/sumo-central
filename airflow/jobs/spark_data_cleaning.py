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
		# include common jars when running with pyspark in notebooks or local mode
		.config(
			"spark.jars.packages",
			"org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
		)
		# Ensure S3A is used and a sensible credentials provider chain is available
		.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
		.config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
		.config("spark.hadoop.fs.s3a.fast.upload", "true")
		.config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
	)

	# Apply driver/executor memory settings if provided
	if driver_mem:
		LOG.info("Setting spark.driver.memory=%s", driver_mem)
		builder = builder.config("spark.driver.memory", driver_mem)
	if executor_mem:
		LOG.info("Setting spark.executor.memory=%s", executor_mem)
		builder = builder.config("spark.executor.memory", executor_mem)

	spark = builder.getOrCreate()

	# Hadoop-level S3A tuning (timeouts, retries, multipart, connection pools)
	hadoop_conf = spark._jsc.hadoopConfiguration()

	# Credentials from environment (optional override)
	aws_key = os.environ.get("AWS_ACCESS_KEY_ID")
	aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
	aws_token = os.environ.get("AWS_SESSION_TOKEN")
	s3_endpoint = os.environ.get("S3_ENDPOINT")
	if aws_key and aws_secret:
		LOG.info("Applying AWS credentials to Hadoop config from environment")
		hadoop_conf.set("fs.s3a.access.key", aws_key)
		hadoop_conf.set("fs.s3a.secret.key", aws_secret)
		if aws_token:
			hadoop_conf.set("fs.s3a.session.token", aws_token)

	# Use AWS V4 signing for newer regions
	hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")

	# Optional custom endpoint (e.g. MinIO or S3 compat)
	if s3_endpoint:
		LOG.info("Setting custom S3 endpoint: %s", s3_endpoint)
		hadoop_conf.set("fs.s3a.endpoint", s3_endpoint)

	# Performance / robustness settings
	hadoop_conf.set("fs.s3a.path.style.access", "true")
	hadoop_conf.set("fs.s3a.connection.maximum", "100")
	hadoop_conf.set("fs.s3a.threads.max", "100")
	hadoop_conf.set("fs.s3a.fast.upload", "true")
	hadoop_conf.set("fs.s3a.fast.upload.buffer", "bytebuffer")
	hadoop_conf.set("fs.s3a.multipart.size", str(10 * 1024 * 1024))         # 10 MB
	hadoop_conf.set("fs.s3a.multipart.threshold", str(64 * 1024 * 1024))    # 64 MB

	# Timeouts and retry tuning (values in milliseconds)
	hadoop_conf.set("fs.s3a.connection.establish.timeout", "30000")  # 30s
	hadoop_conf.set("fs.s3a.connection.timeout", "60000")          # 60s
	hadoop_conf.set("fs.s3a.threads.keepalivetime", "60000")       # 60s
	hadoop_conf.set("fs.s3a.connection.ttl", "300000")             # 5m
	hadoop_conf.set("fs.s3a.retry.interval", "500")               # 500ms
	hadoop_conf.set("fs.s3a.retry.throttle.interval", "100")       # 100ms

	# When assuming roles, set a reasonable session duration (ms)
	hadoop_conf.set("fs.s3a.assumed.role.session.duration", "1800000")  # 30m

	# Try to avoid native library dependency surprises in container / Windows dev
	hadoop_conf.set("spark.hadoop.io.native.lib.available", "false")

	return spark


def process_data(
	input_path: str,
	output_path: str,
	input_format: str = "parquet",
	output_format: str = "parquet",
	partition_by: Optional[list[str]] = None,
	dropna_thresh: Optional[int] = None,
	app_name: str = "data_cleaning_job",
	driver_memory: Optional[str] = None,
	executor_memory: Optional[str] = None,
) -> None:
	LOG.info("Starting process_data: %s -> %s", input_path, output_path)
	spark = create_spark_session(app_name=app_name, driver_memory=driver_memory, executor_memory=executor_memory)

	try:
		print("starting process")
		# Try to detect S3A implementation on the JVM, but be tolerant of
		# transient classloader timing when using Ivy (--packages). Retry a few
		# times before giving up, and only log a warning rather than failing
		# immediately. If S3A is not available later, the actual read/write
		# operations will fail with a clear Java stack trace.
		found_s3a = False
		for attempt in range(6):
			try:
				spark._jvm.java.lang.Class.forName("org.apache.hadoop.fs.s3a.S3AFileSystem")
				found_s3a = True
				print("found s3a")
				break
			except Exception as _err:  # pragma: no cover - runtime check
				# On first few attempts, sleep briefly to allow Ivy to add jars
				# into the driver classpath in client mode.
				LOG.debug("S3AFileSystem not found (attempt %s): %s", attempt + 1, _err)
				time.sleep(0.5)

		if not found_s3a:
			LOG.warning(
				"S3AFileSystem class not found in JVM after retries. Ensure Spark has hadoop-aws and aws-java-sdk on the classpath (use --packages or pre-bake jars into /opt/spark/jars). Further failures during S3 reads/writes will surface in the job logs.")

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
	p.add_argument(
		"--input",
		required=False,
		default="s3a://ryans-sumo-bucket/sumo-api-calls/rikishi_matches/",
		help="Primary input path (default matches internal job reads)",
	)
	p.add_argument(
		"--output",
		required=False,
		default="s3a://ryans-sumo-bucket/silver/rikishi_matches/",
		help="Primary output path (default matches internal silver write)",
	)
	p.add_argument("--input-format", default="parquet", choices=["parquet", "csv"]) 
	p.add_argument("--output-format", default="parquet", choices=["parquet", "csv"]) 
	p.add_argument("--partition-by", nargs="*", help="columns to partition by")
	p.add_argument("--dropna-thresh", type=int)
	p.add_argument("--app-name", default="data_cleaning_job")
	p.add_argument("--driver-memory", help="Spark driver memory (e.g. 4g). Can also be set with SPARK_DRIVER_MEMORY env var")
	p.add_argument("--executor-memory", help="Spark executor memory (e.g. 4g). Can also be set with SPARK_EXECUTOR_MEMORY env var")
	return p.parse_args()


if __name__ == "__main__":
	args = _parse_args()
	process_data(
		input_path=args.input,
		output_path=args.output,
		input_format=args.input_format,
		output_format=args.output_format,
		partition_by=args.partition_by,
		dropna_thresh=args.dropna_thresh,
		app_name=args.app_name,
		driver_memory=args.driver_memory,
		executor_memory=args.executor_memory,
	)




