from __future__ import annotations

import argparse
import logging
import os
from typing import Optional

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *


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


def process_ml_dataset(
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
        df = (spark.read
              .option("multiLine", "true")                # set to "false" if you have JSON Lines
              .option("mode", "PERMISSIVE")               # or "FAILFAST"
              .option("recursiveFileLookup", "true")
              .option("pathGlobFilter", "*.parquet")         # only JSON files
              .parquet("s3a://ryans-sumo-bucket/silver/rikishi_matches/"))
        print("read matches")
        matches = (df
                   .withColumn(
                       "westWin",
                       F.when(F.col("winnerId") == F.col("westId"), F.lit(1)).otherwise(F.lit(0))
                   )
                   )

        rikishis = (spark.read
                    .option("multiLine", "true")                # set to "false" if you have JSON Lines
                    .option("mode", "PERMISSIVE")               # or "FAILFAST"
                    .option("recursiveFileLookup", "true")
                    .option("pathGlobFilter", "*.parquet")         # only JSON files
                    .parquet("s3a://ryans-sumo-bucket/silver/rikishis/"))
        print("read rikishis")
        rikishi_stats = (spark.read
                         .option("multiLine", "true")                # set to "false" if you have JSON Lines
                         .option("mode", "PERMISSIVE")               # or "FAILFAST"
                         .option("recursiveFileLookup", "true")
                         .option("pathGlobFilter", "*.parquet")         # only JSON files
                         .parquet("s3a://ryans-sumo-bucket/silver/rikishi_stats/"))

        print("read rikishi stats")
        bashos = (spark.read
                  .option("multiLine", "true")                # set to "false" if you have JSON Lines
                  .option("mode", "PERMISSIVE")               # or "FAILFAST"
                  .option("recursiveFileLookup", "true")
                  .option("pathGlobFilter", "*.parquet")         # only JSON files
                  .parquet("s3a://ryans-sumo-bucket/silver/bashos/"))
        print("read bashos")
        # Rename `date` → `bashoId`, and keep start/end date
        base = bashos.select(
            col("date").alias("bashoId"),
            col("startDate"),
            col("endDate")
        )

        yusho_flat = (
            bashos.withColumn("y", explode(col("yusho")))
            .select(
                col("date").alias("bashoId"),
                col("y.type").alias("yusho_type"),
                col("y.rikishiId").alias("rikishiId")
            )
        )

        # Pivot so each yusho_type becomes its own column
        yusho_pivot = (
            yusho_flat.groupBy("bashoId")
            .pivot("yusho_type")
            .agg(collect_list("rikishiId"))
            .withColumnRenamed("Makuuchi", "Makuuchi_yusho")
            .withColumnRenamed("Juryo", "Juryo_yusho")
        )
        prizes_flat = (
            bashos.withColumn("p", explode(col("specialPrizes")))
            .select(
                col("date").alias("bashoId"),
                col("p.type").alias("prize_type"),
                col("p.rikishiId").alias("rikishiId")
            )
        )


        prizes_pivot = (
            prizes_flat.groupBy("bashoId")
            .pivot("prize_type")
            .agg(collect_list("rikishiId"))   # array of rikishiIds
            .withColumnRenamed("Shukun-sho", "Shukun_sho")
            .withColumnRenamed("Kanto-sho", "Kanto_sho")
            .withColumnRenamed("Gino-sho", "Gino_sho")
        )
        result = (base
                  .join(yusho_pivot, on="bashoId", how="left")
                  .join(prizes_pivot, on="bashoId", how="left"))

        rikishi_stats = rikishi_stats.select(
            col("rikishi_id"),
            col("yusho"),
            col("yushoByDivision.Makuuchi").alias("makuuchi_yusho"),
            col("totalWins"),
            col("totalLosses"),
            col("totalMatches"),
            col("winsByDivision.Makuuchi").alias("makuuchiWins"),
            col("basho"),
            col("bashoByDivision.Makuuchi").alias("Makuuchi_basho"),
            col("sansho.Gino-sho").alias("Gino_sho"),
            col("sansho.Kanto-sho").alias("Kanto_sho"),
            col("sansho.Shukun-sho").alias("Shukun_sho")
        )

        main = matches

        rikishis = (rikishis
                    .join(rikishi_stats, rikishis.id == rikishi_stats.rikishi_id, how = "left"))

        rikishis = (rikishis
                    .drop("shikonaEn")
                    .drop("shikonaJp")
                    .drop("sumodbId")
                    .drop("nskId")
                    .drop("shusshin")
                    .drop("heya")
                    )

        def add_prefix(df, prefix):
            return df.select([F.col(c).alias(f"{prefix}_{c}") for c in df.columns])

        # Add prefixes
        west = add_prefix(rikishis, "west")
        east = add_prefix(rikishis, "east")

        # Join with prefixed columns
        main = (
            matches
            .join(west, matches.westId == west.west_id, "left")
            .join(east, matches.eastId == east.east_id, "left")
        )

        ranks = (spark.read
                 .option("multiLine", "true")                # set to "false" if you have JSON Lines
                 .option("mode", "PERMISSIVE")               # or "FAILFAST"
                 .option("recursiveFileLookup", "true")
                 .option("pathGlobFilter", "*.json")         # only JSON files
                 .json("s3a://ryans-sumo-bucket/sumo-api-calls/rikishi_ranks/"))
        print("read ranks")
        rows = ranks.select(explode(col("records")).alias("rank"))
        flat = rows.select("rank.*")   # expand struct into columns
        ranks = (flat.select("rank", "rankValue").dropDuplicates())

        filtered = ranks.filter(F.col("rank") != "")

        # Add a flag: east = 0 (better), west = 1 (worse)
        with_side = filtered.withColumn(
            "side_flag",
            F.when(F.col("rank").like("%East"), F.lit(0))
            .when(F.col("rank").like("%West"), F.lit(1))
            .otherwise(F.lit(2))  # fallback if no side is present
        )

        # Window: order by rankValue asc, then side_flag asc
        w = Window.orderBy("rankValue", "side_flag")

        # Assign sequential ordering
        ordered_ranks = (
            with_side
            .withColumn("order", F.row_number().over(w))
            .select("rank", "rankValue", "order")
        )

        west = add_prefix(ordered_ranks, "west")
        east = add_prefix(ordered_ranks, "east")

        main = (main
                .join(west, main.westRank == west.west_rank, "left")
                .join(east, main.eastRank == east.east_rank, "left")
                )

        # 1️⃣ Create new parsed date columns safely
        main = main.withColumn(
            "east_birthdate_parsed",
            F.to_date(F.regexp_replace(F.col("east_birthdate"), "T.*Z", ""), "yyyy-MM-dd")
        ).withColumn(
            "west_birthdate_parsed",
            F.to_date(F.regexp_replace(F.col("west_birthdate"), "T.*Z", ""), "yyyy-MM-dd")
        ).withColumn(
            "match_date_parsed",
            F.to_date(F.col("match_date"), "yyyy-MM-dd")
        )

        # 2️⃣ Drop original ISO 8601 string columns to avoid automatic timestamp parsing
        main = main.drop("east_birthdate", "west_birthdate", "match_date")

        # 3️⃣ Calculate ages based on parsed dates
        main = main.withColumn(
            "east_age",
            F.floor(F.months_between(F.col("match_date_parsed"), F.col("east_birthdate_parsed")) / 12)
        ).withColumn(
            "west_age",
            F.floor(F.months_between(F.col("match_date_parsed"), F.col("west_birthdate_parsed")) / 12)
        )

        # ✅ Optional: rename parsed match date to original name if needed
        main = main.withColumnRenamed("match_date_parsed", "match_date")

        # 1️⃣ Normalize debut strings: handle YYYYMM format
        main = main.withColumn(
            "east_debut_clean",
            F.when(F.length("east_debut") == 6,
                   F.concat_ws("-", F.col("east_debut").substr(1,4), F.col("east_debut").substr(5,2), F.lit("01"))
                   ).otherwise(F.col("east_debut"))
        ).withColumn(
            "west_debut_clean",
            F.when(F.length("west_debut") == 6,
                   F.concat_ws("-", F.col("west_debut").substr(1,4), F.col("west_debut").substr(5,2), F.lit("01"))
                   ).otherwise(F.col("west_debut"))
        )

        # 2️⃣ Strip ISO 8601 T/Z if present and parse to date
        main = main.withColumn(
            "east_debut_parsed",
            F.to_date(F.regexp_replace("east_debut_clean", "T.*Z", ""), "yyyy-MM-dd")
        ).withColumn(
            "west_debut_parsed",
            F.to_date(F.regexp_replace("west_debut_clean", "T.*Z", ""), "yyyy-MM-dd")
        )

        # 3️⃣ Calculate years active
        main = main.withColumn(
            "east_years_active",
            F.floor(F.months_between("match_date", "east_debut_parsed") / 12)
        ).withColumn(
            "west_years_active",
            F.floor(F.months_between("match_date", "west_debut_parsed") / 12)
        )

        (main.write
         .mode("overwrite")
         .option("compression", "snappy")
         .parquet("s3a://ryans-sumo-bucket/gold/ml_training/"))
        print("wrote ml training dataset")
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
	process_ml_dataset(
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




