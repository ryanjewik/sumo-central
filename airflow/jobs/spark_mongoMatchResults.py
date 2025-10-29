import os
import sys
import json
from datetime import datetime


def process_match_results(webhook_payload):
    """Compact skeleton: build a Spark DataFrame from webhook payload and
    show how to configure the Mongo connector. Minimal fallback to pandas.
    """
    # parse if necessary
    if isinstance(webhook_payload, str):
        webhook = json.loads(webhook_payload)
    else:
        webhook = webhook_payload or {}

    matches = webhook.get("payload_decoded") or []

    # Try Spark path
    try:
        from pyspark.sql import SparkSession

        mongo_uri = os.environ.get("MONGO_URI")
        db_name = os.environ.get("MONGO_DB_NAME") or "test"
        coll_name = os.environ.get("MONGO_COLL_NAME") or "match_results"

        builder = SparkSession.builder.appName("spark_mongo_match_results")
        # If a Mongo URI is provided, pass it to Spark's config for the Mongo Connector
        if mongo_uri:
            # common configuration key for the Mongo Spark Connector
            builder = builder.config("spark.mongodb.output.uri", mongo_uri)

        spark = builder.getOrCreate()

        # Build DataFrame from Python list of dicts
        df = spark.createDataFrame(matches)

        # Example: light casting and a derived column (expand as needed)
        from pyspark.sql import functions as F

        df = df.withColumn("matchNo", F.col("matchNo").cast("int"))
        df = df.withColumn("division_lower", F.lower(F.coalesce(F.col("division"), F.lit(""))))

        # Show schema and a few rows — useful during development
        df.printSchema()
        df.show(5, truncate=False)

        # Write with the Mongo Spark Connector (requires the connector on Spark classpath)
        # If connector jar is present and spark is configured for it, this will write to Mongo.
        if mongo_uri:
            df.write.format("mongo") \
                .mode("append") \
                .option("uri", mongo_uri) \
                .option("database", db_name) \
                .option("collection", coll_name) \
                .save()

        return df

    except Exception:
        # Minimal fallback: return a pandas DataFrame if possible
        try:
            import pandas as pd
            pdf = pd.DataFrame(matches)
            print("Pandas DataFrame shape:", pdf.shape)
            return pdf
        except Exception:
            # Last-resort: return the raw list
            print("Spark/pandas not available; returning raw match list")
            return matches


def main():
    if len(sys.argv) < 2:
        print("Provide webhook JSON as first arg")
        sys.exit(2)

    payload = json.loads(sys.argv[1])
    res = process_match_results(payload)
    print("Processed result type:", type(res))


if __name__ == "__main__":
    main()
