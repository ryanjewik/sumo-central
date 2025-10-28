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
from pyspark.sql.functions import (
    explode,
    col,
    when,
    to_date,
    to_timestamp,
    date_add,
    concat,
    substring,
    lpad,
    lit,
    date_format,
)
from pyspark.sql.types import StringType, LongType
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

    mongo_uri = os.environ.get("MONGO_URI")
    mongo_db = os.environ.get("MONGO_DB_NAME")
    if not mongo_uri:
        raise RuntimeError("MONGO_URI must be provided in the executor environment (spark.executorEnv.MONGO_URI)")
    if not mongo_db:
        raise RuntimeError("MONGO_DB_NAME must be provided in the executor environment (spark.executorEnv.MONGO_DB_NAME)")

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
    print(f"Loaded {df.count()} matches from webhook payload.")


    if mongo_uri:
        try:
            first_basho = None
            try:
                first_row = df.select("bashoId").where(col("bashoId").isNotNull()).limit(1).collect()
                if first_row:
                    first_basho = first_row[0][0]
            except Exception:
                # If anything goes wrong collecting the first id, fall back
                # to None and the code below will produce an empty selection.
                first_basho = None

            numeric_id = None
            if first_basho is not None:
                print(f"passing basho id for join: {first_basho}")
                try:
                    numeric_id = int(first_basho)
                except Exception:
                    numeric_id = None

            reader = (
                spark.read.format("mongo")
                .option("uri", mongo_uri)
                .option("database", mongo_db)
                .option("collection", "basho_pages")
            )

            pipeline_clauses = []
            if numeric_id is not None:
                pipeline_clauses = [{"$match": {"id": numeric_id}}]
                pipeline_json = _json.dumps(pipeline_clauses)
                reader = reader.option("pipeline", pipeline_json).option("allowDiskUse", "true")

            try:
                if pipeline_clauses:
                    basho_pages = reader.load()
                    print("successfully loaded basho_pages for join")
                    print(basho_pages.show(1))
                else:
                    # No numeric bashoId available in payload — create empty DataFrame
                    # and skip hitting the remote collection.
                    basho_pages = spark.createDataFrame([], schema=None)
                    print("no numeric bashoId; skipping basho_pages read")
            except Exception as e:
                # Connector read failed (e.g., server sort memory limit). Try a
                # small pymongo driver fetch as a last-resort fallback.
                msg = str(e)
                print(f"Mongo connector read failed: {msg}")

            # Normalize possible locations for basho id and start_date and coalesce them
            from pyspark.sql.functions import coalesce, trim

            basho_df = basho_pages.select(
                col("basho.start_date").alias("basho_start_date"),
                col("id").cast(LongType()).alias("basho_id_int"),
                col("id").cast(StringType()).alias("basho_id_str"),
            )

            # Trim whitespace from string ids and select only necessary columns
            basho_df = basho_df.withColumn("basho_id_str", trim(col("basho_id_str"))).select("basho_id_str", "basho_start_date", "basho_id_int")
            df = df.join(basho_df, df.bashoId == basho_df.basho_id_str, how="left").select(df["*"], basho_df["basho_start_date"])
        except Exception as e:
            print(f"failed to join basho start dates: {e}")
            pass

    try:
        from pyspark.sql import functions as F
        df = df.withColumn("basho_start_date", F.to_date(F.col("basho_start_date")))

        df = df.withColumn(
            "match_date_dt",
            F.when(
                F.col("day").isNotNull() & F.col("basho_start_date").isNotNull(),
                F.expr("date_add(basho_start_date, cast(day as int) - 1)"),
            )
        )

        df = df.withColumn(
            "match_date",
            F.expr("concat(date_format(match_date_dt, 'yyyy-MM-dd'), 'T00:00:00')"),
        )

        cols_to_remove = []
        for col_name in [
            "match_date_dt",
            "basho_id_int",
            "basho_id_str",
            "basho_start_date",
            "match_date_iso",
            ]:
            if col_name in df.columns:
                cols_to_remove.append(col_name)

        if cols_to_remove:
            df = df.drop(*cols_to_remove)
    except Exception as e:
        print(f"failed to compute match_date: {e}")

    # stage for upload and upload
    if mongo_uri:
        numeric_id_for_update = None
        try:
            numeric_id_for_update = locals().get("numeric_id", None)
        except Exception:
            numeric_id_for_update = None
        if numeric_id_for_update is not None:
            try:
                from pymongo import MongoClient, UpdateOne

                def make_upsert_partition(mongo_uri_inner, mongo_db_inner, numeric_id_inner):
                    def upsert_partition(rows_iter):
                        # This function runs inside executors/partitions
                        try:
                            client = MongoClient(mongo_uri_inner)
                            db = client.get_database(mongo_db_inner)
                            coll = db["basho_pages"]

                            groups = {}
                            for r in rows_iter:
                                d = r.asDict()
                                match_date_iso = d.get("match_date")
                                if not match_date_iso:
                                    continue
                                date_key = match_date_iso[:10]
                                division = d.get("division") or "Unknown"
                                match_doc = {
                                    "match_date": match_date_iso,
                                    "match_number": int(d.get("matchNo")) if d.get("matchNo") not in (None, "") else None,
                                    "eastshikona": d.get("eastShikona"),
                                    "westshikona": d.get("westShikona"),
                                    "division": division,
                                    "winner": int(d.get("winnerId")) if d.get("winnerId") not in (None, "") else None,
                                    "kimarite": d.get("kimarite"),
                                    "east_rikishi_id": int(d.get("eastId")) if d.get("eastId") not in (None, "") else None,
                                    "west_rikishi_id": int(d.get("westId")) if d.get("westId") not in (None, "") else None,
                                }
                                key = (division, date_key)
                                groups.setdefault(key, []).append(match_doc)

                            ops = []
                            for (division_k, date_k), matches in groups.items():
                                update_path = f"days.{division_k}.{date_k}"
                                ops.append(UpdateOne({"id": numeric_id_inner}, {"$push": {update_path: {"$each": matches}}}))
                            # Also prepare updates for rikishi_pages.upcoming__match
                            rikishi_coll = db.get_collection("rikishi_pages")
                            rikishi_ops = []
                            # While we built grouped matches for basho_pages above, we also
                            # need a per-row match object to set as upcoming__match for
                            # the east and west rikishi. We'll iterate groups' matches
                            # (they contain the compact match objects) and create set
                            # operations keyed by rikishi id.
                            for (division_k, date_k), matches in groups.items():
                                for m in matches:
                                    # prefer explicit integer rikishi ids if available
                                    east_id = m.get("east_rikishi_id")
                                    west_id = m.get("west_rikishi_id")
                                    # Build the match object to store verbatim under upcoming__match
                                    match_obj = {
                                        "match_date": m.get("match_date"),
                                        "match_number": m.get("match_number"),
                                        "eastshikona": m.get("eastshikona"),
                                        "westshikona": m.get("westshikona"),
                                        "division": m.get("division"),
                                        "winner": m.get("winner"),
                                        "kimarite": m.get("kimarite"),
                                        "east_rikishi_id": m.get("east_rikishi_id"),
                                        "west_rikishi_id": m.get("west_rikishi_id"),
                                    }
                                    try:
                                        if east_id not in (None, ""):
                                            rikishi_ops.append(
                                                UpdateOne({"id": int(east_id)}, {"$set": {"upcoming__match": match_obj}})
                                            )
                                    except Exception:
                                        # fallback to string id if cannot cast
                                        try:
                                            rikishi_ops.append(
                                                UpdateOne({"id": east_id}, {"$set": {"upcoming__match": match_obj}})
                                            )
                                        except Exception:
                                            pass
                                    try:
                                        if west_id not in (None, "") and west_id != east_id:
                                            rikishi_ops.append(
                                                UpdateOne({"id": int(west_id)}, {"$set": {"upcoming__match": match_obj}})
                                            )
                                    except Exception:
                                        try:
                                            if west_id not in (None, "") and west_id != east_id:
                                                rikishi_ops.append(
                                                    UpdateOne({"id": west_id}, {"$set": {"upcoming__match": match_obj}})
                                                )
                                        except Exception:
                                            pass

                            if ops:
                                coll.bulk_write(ops, ordered=False)
                            if rikishi_ops:
                                try:
                                    rikishi_coll.bulk_write(rikishi_ops, ordered=False)
                                except Exception:
                                    # If rikishi updates fail, don't block basho_pages updates;
                                    # allow the exception to surface if needed upstream.
                                    raise
                            client.close()
                        except Exception:
                            raise

                    return upsert_partition
                
                if "division" in df.columns:
                    try:
                        df = df.repartition(col("division"))
                    except Exception as e:
                        print(f"Repartition by division failed, continuing: {e}")

                # Trigger foreachPartition; this will execute on executors
                df.rdd.foreachPartition(make_upsert_partition(mongo_uri, mongo_db, numeric_id_for_update))
                print(f"Executor-side pymongo: scheduled partitioned updates for basho_pages.id={numeric_id_for_update}")
            except Exception as e:
                print(f"Executor-side pymongo write failed to schedule/execute: {e}")

    else:
        print("MONGO_URI not set; skipping write to MongoDB. Sample output:")
        for r in df.take(20):
            print(r)

    spark.stop()


if __name__ == "__main__":
    main()
