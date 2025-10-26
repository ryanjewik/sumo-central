#!/usr/bin/env python3
"""
Spark job to process webhook matches and write them to a Mongo staging collection
using the Mongo Spark Connector.

This job writes each match as a document to `basho_matches_staging` so a later
merge/aggregation step (or a small Mongo script) can perform the $push updates
into the existing `basho_pages.days.<division>.<date>` arrays and update
`rikishi_pages.upcoming_matches`.

Usage:
  spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
      spark_mongoNewMatches.py [/path/to/webhook.json]

If no path is provided the job will attempt to read
`/opt/airflow/jobs/latest_webhook.json`.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StringType
import os
import sys
from datetime import datetime, timedelta


def compute_match_date(basho_id: str, day: int, start_date: str = None) -> str:
    try:
        if start_date:
            base = datetime.strptime(start_date[:10], "%Y-%m-%d")
            return (base + timedelta(days=int(day) - 1)).strftime("%Y-%m-%dT00:00:00")
    except Exception:
        pass
    try:
        year = int(basho_id[:4])
        month = int(basho_id[4:6])
        return datetime(year, month, int(day)).strftime("%Y-%m-%dT00:00:00")
    except Exception:
        return None


def main():
    app_name = "spark_mongoNewMatches"
    if len(sys.argv) > 1:
        webhook_json_arg = sys.argv[1]
    else:
        print("Expected webhook JSON as first argument (from XCom).")
        # We haven't created a SparkSession yet, so just exit.
        sys.exit(2)

    mongo_uri = os.getenv("MONGO_URI")
    mongo_db = os.getenv("MONGO_DB_NAME", "sumo")

    # Create SparkSession and instruct Spark to pull the Mongo Spark Connector
    # at runtime via the jars packages setting. We also set the default
    # mongo input/output URI so that the connector uses them by default.
    builder = SparkSession.builder.appName(app_name)
    if mongo_uri:
        # Note: this will cause Spark to download the connector from Maven
        # when the session starts. Ensure the container can reach Maven Central
        # or pre-bundle the jar into the image (see docker-compose notes).
        builder = builder.config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        ).config("spark.mongodb.output.uri", mongo_uri).config(
            "spark.mongodb.input.uri", mongo_uri
        )

    spark = builder.getOrCreate()

    # webhook_json_arg is expected to be a JSON string passed via XCom/application_args.
    import json as _json
    try:
        payload_obj = _json.loads(webhook_json_arg)
    except Exception as e:
        print(f"Failed to parse webhook JSON from argument: {e}")
        spark.stop()
        sys.exit(2)

    if isinstance(payload_obj, dict) and "payload_decoded" in payload_obj:
        matches = payload_obj["payload_decoded"]
    elif isinstance(payload_obj, list):
        matches = payload_obj
    else:
        matches = []

    df = spark.createDataFrame(matches)

    # Add computed match_date column (use pandas UDF or simple map on rdd)
    def row_to_dict(row):
        d = row.asDict()
        basho_id = str(d.get("bashoId")) if d.get("bashoId") is not None else ""
        day = d.get("day") or 0
        start_date = None
        # We don't fetch nested basho.start_date here; that's a follow-up optimization.
        match_date_iso = compute_match_date(basho_id, day, start_date)
        d["match_date_iso"] = match_date_iso
        return d

    rdd = df.rdd.map(row_to_dict)

    # Convert back to dataframe
    df2 = spark.createDataFrame(rdd)

    # Write out to a staging collection via the Mongo Spark Connector
    if mongo_uri:
        df2.write.format("mongo") \
            .option("uri", mongo_uri) \
            .option("database", mongo_db) \
            .option("collection", "basho_matches_staging") \
            .mode("append") \
            .save()
        print("Wrote matches to staging collection 'basho_matches_staging'")
    else:
        print("MONGO_URI not set; skipping write to MongoDB. Sample output:")
        for r in df2.take(20):
            print(r)

    spark.stop()


if __name__ == "__main__":
    main()
