#!/usr/bin/env python3
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
    import json as _json
    app_name = "spark_mongoNewMatches"
    if len(sys.argv) <= 1:
        print("[spark_mongoNewMatches] no args passed from Airflow")
        return {}

    # Accept arguments in multiple ways for robustness:
    #  - --payload-file <local_path>
    #  - --payload-s3 <s3a://bucket/key.json>
    #  - legacy: a single (possibly very long) JSON payload passed as CLI args
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--payload-file", dest="payload_file", help="Local file path containing webhook JSON")
    parser.add_argument("--payload-s3", dest="payload_s3", help="S3 a URI (s3a://bucket/key) containing webhook JSON")
    parser.add_argument("legacy", nargs="*", help="Legacy positional JSON parts")
    args = parser.parse_args()

    webhook_payload = None
    # 1) payload-file (local)
    if args.payload_file:
        try:
            with open(args.payload_file, "r", encoding="utf-8") as fh:
                raw = fh.read()
            webhook_payload = _json.loads(raw)
        except Exception as e:
            print(f"[spark_mongoNewMatches] failed to read/parse --payload-file {args.payload_file}: {e}")
            sys.exit(2)

    # 2) payload-s3 (fetch via boto3)
    elif args.payload_s3:
        try:
            # normalize s3a:// or s3:// → bucket/key
            s3uri = args.payload_s3
            s3_norm = s3uri.replace("s3a://", "s3://")
            if s3_norm.startswith("s3://"):
                s3_norm = s3_norm[5:]
            parts = s3_norm.split("/", 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ""
            import boto3
            client = boto3.client("s3")
            resp = client.get_object(Bucket=bucket, Key=key)
            raw = resp["Body"].read().decode("utf-8")
            webhook_payload = _json.loads(raw)
        except Exception as e:
            print(f"[spark_mongoNewMatches] failed to fetch/parse --payload-s3 {args.payload_s3}: {e}")
            sys.exit(2)

    # 3) legacy: join CLI parts and parse JSON (backwards compatibility)
    else:
        raw = " ".join(args.legacy or []).strip()
        print("[spark_mongoNewMatches] legacy raw arg:", raw[:300], "...")
        try:
            webhook_payload = _json.loads(raw) if raw else {}
        except Exception as e:
            print("[spark_mongoNewMatches] failed to parse JSON (legacy):", e)
            webhook_payload = {}
            sys.exit(2)

    mongo_uri = os.environ.get("MONGO_URI")
    mongo_db = os.environ.get("MONGO_DB_NAME")
    if not mongo_uri:
        raise RuntimeError("MONGO_URI must be provided in the executor environment (spark.executorEnv.MONGO_URI)")
    if not mongo_db:
        raise RuntimeError("MONGO_DB_NAME must be provided in the executor environment (spark.executorEnv.MONGO_DB_NAME)")
    
    builder = SparkSession.builder.appName(app_name)
    if mongo_uri:
        builder = builder.config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        ).config("spark.mongodb.output.uri", mongo_uri).config(
            "spark.mongodb.input.uri", mongo_uri
        )

    spark = builder.getOrCreate()


    if isinstance(webhook_payload, str):
        webhook = _json.loads(webhook_payload)
    else:
        webhook = webhook_payload or {}

    # 2. accept multiple shapes:
    #    - {"payload_decoded": [...]}
    #    - {"payload": [...]}
    #    - [...]
    if isinstance(webhook, dict):
        matches = (
            webhook.get("payload_decoded")
            or webhook.get("payload")
            or []
        )
    elif isinstance(webhook, list):
        matches = webhook
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

            # If we attempted to read a specific basho by numeric id but got
            # an empty result, proactively create the minimal basho_pages
            # document on the Mongo server so later upserts can assume it
            # exists. This matches the structure produced by mongoNewBasho.
            try:
                if pipeline_clauses and numeric_id is not None:
                    empty = False
                    try:
                        # If reader.load succeeded above, basho_pages will be a DF.
                        # Check if it's empty. Use rdd.isEmpty() to avoid expensive
                        # full-count where possible.
                        if 'basho_pages' in locals() and hasattr(basho_pages, 'rdd'):
                            empty = basho_pages.rdd.isEmpty()
                        else:
                            empty = True
                    except Exception:
                        # If checking emptiness fails, fall back to treating as empty
                        empty = True

                    if empty:
                        try:
                            from pymongo import MongoClient

                            client = MongoClient(mongo_uri)
                            db = client[mongo_db]
                            coll = db.get_collection('basho_pages')

                            existing = coll.find_one({'id': numeric_id})
                            if not existing:
                                # Build minimal basho document. Prefer authoritative
                                # values from Postgres, falling back to webhook values.
                                start_date = None
                                end_date = None
                                location = None

                                # Try to fetch basho metadata from Postgres first
                                try:
                                    # Try PostgresHook first (Airflow environment). We only
                                    # query using the numeric id (integer) as configured.
                                    start_date = None
                                    end_date = None
                                    location = None
                                    try:
                                        from airflow.providers.postgres.hooks.postgres import PostgresHook
                                        pg_conn_id = os.getenv('POSTGRES_CONN_ID', 'postgres_default')
                                        ph = PostgresHook(postgres_conn_id=pg_conn_id)
                                        pconn = ph.get_conn()
                                        pcur = pconn.cursor()
                                        pcur.execute('SELECT start_date, end_date, location FROM basho WHERE id = %s', (numeric_id,))
                                        row = pcur.fetchone()
                                        try:
                                            pcur.close()
                                        except Exception:
                                            pass
                                        try:
                                            pconn.close()
                                        except Exception:
                                            pass
                                        if row:
                                            start_date, end_date, location = row[0], row[1], row[2]
                                            print(f"Postgres lookup returned row for basho id={numeric_id}: start_date={start_date}, end_date={end_date}, location={location}")
                                        else:
                                            print(f"Postgres lookup returned no row for basho id={numeric_id}")
                                    except Exception:
                                        # Fall back to psycopg2 using env vars / DATABASE_URL
                                        import psycopg2
                                        pg_urls = os.getenv('DATABASE_URL') or os.getenv('POSTGRES_URL')
                                        if pg_urls:
                                            pconn = psycopg2.connect(pg_urls)
                                        else:
                                            pg_host = os.getenv('POSTGRES_HOST')
                                            pg_port = os.getenv('POSTGRES_PORT')
                                            pg_db = os.getenv('POSTGRES_DB') or os.getenv('POSTGRES_DATABASE')
                                            pg_user = os.getenv('POSTGRES_USER')
                                            pg_pass = os.getenv('POSTGRES_PASSWORD')
                                            conn_args = {}
                                            if pg_host:
                                                conn_args['host'] = pg_host
                                            if pg_port:
                                                conn_args['port'] = pg_port
                                            if pg_db:
                                                conn_args['dbname'] = pg_db
                                            if pg_user:
                                                conn_args['user'] = pg_user
                                            if pg_pass:
                                                conn_args['password'] = pg_pass
                                            pconn = psycopg2.connect(**conn_args)
                                        pcur = pconn.cursor()
                                        pcur.execute('SELECT start_date, end_date, location FROM basho WHERE id = %s', (numeric_id,))
                                        row = pcur.fetchone()
                                        try:
                                            pcur.close()
                                        except Exception:
                                            pass
                                        try:
                                            pconn.close()
                                        except Exception:
                                            pass
                                        if row:
                                            start_date, end_date, location = row[0], row[1], row[2]
                                            print(f"Postgres lookup returned row for basho id={numeric_id}: start_date={start_date}, end_date={end_date}, location={location}")
                                        else:
                                            print(f"Postgres lookup returned no row for basho id={numeric_id}")
                                except Exception:
                                    # Ignore DB lookup failures and fall back to webhook values below
                                    print(f"Postgres lookup failed for basho id={numeric_id}; falling back to webhook/api")

                                # webhook may contain top-level payload with startDate/location fields
                                try:
                                    if (not start_date or not end_date or not location) and isinstance(webhook, dict):
                                        raw_payload = None
                                        if 'payload_decoded' in webhook:
                                            raw_payload = webhook.get('payload_decoded')
                                        elif 'payload' in webhook:
                                            raw_payload = webhook.get('payload')
                                        else:
                                            raw_payload = webhook

                                        # raw_payload may be dict or list
                                        if isinstance(raw_payload, dict):
                                            start_date = start_date or raw_payload.get('startDate') or raw_payload.get('start_date')
                                            end_date = end_date or raw_payload.get('endDate') or raw_payload.get('end_date')
                                            location = location or raw_payload.get('location')
                                        elif isinstance(raw_payload, list) and len(raw_payload) > 0 and isinstance(raw_payload[0], dict):
                                            first = raw_payload[0]
                                            start_date = start_date or first.get('startDate') or first.get('start_date')
                                            end_date = end_date or first.get('endDate') or first.get('end_date')
                                            location = location or first.get('location')
                                except Exception:
                                    pass

                                # fallback to first match entry if still missing
                                try:
                                    if (not start_date or not end_date or not location) and isinstance(matches, list) and len(matches) > 0:
                                        firstm = matches[0]
                                        start_date = start_date or firstm.get('startDate') or firstm.get('start_date')
                                        end_date = end_date or firstm.get('endDate') or firstm.get('end_date')
                                        location = location or firstm.get('location')
                                except Exception:
                                    pass

                                # Normalize dates to YYYY-MM-DD when present
                                try:
                                    if start_date:
                                        start_date = str(start_date)[:10]
                                except Exception:
                                    start_date = None
                                try:
                                    if end_date:
                                        end_date = str(end_date)[:10]
                                except Exception:
                                    end_date = None

                                basho_name = f"Basho {first_basho}" if first_basho is not None else f"Basho {numeric_id}"

                                payload = {
                                    'id': numeric_id,
                                    'basho': {
                                        'basho_id': numeric_id,
                                        'basho_name': basho_name,
                                        'location': location,
                                        'start_date': start_date,
                                        'end_date': end_date,
                                        'makuuchi_yusho': None,
                                        'juryo_yusho': None,
                                        'sandanme_yusho': None,
                                        'makushita_yusho': None,
                                        'jonidan_yusho': None,
                                        'jonokuchi_yusho': None,
                                    },
                                    'days': {},
                                }

                                # Insert only the minimal basho_pages skeleton (no matches).
                                try:
                                    coll.insert_one(payload)
                                    print(f"Inserted initial basho_pages document for id={numeric_id}")
                                except Exception as exc:
                                    print(f"Failed to insert basho_pages doc for id={numeric_id}: {exc}")

                            client.close()
                        except Exception as exc:
                            print(f"Failed to create basho_pages document: {exc}")
            except Exception:
                # non-fatal; continue without creating a doc
                raise

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
                # Try vectorized driver-side scoring first: read cleaned snapshots from S3,
                # rebuild the training-style features, show the read DF and the score DF,
                # then call the saved PipelineModel on the driver. If anything fails,
                # fall back to per-row match_predict in the executor partitioner below.
                from pyspark.sql import functions as F
                from pyspark.sql.window import Window
                from pyspark.sql.functions import row_number
                from pyspark.ml import PipelineModel as _PipelineModel
                from pymongo import MongoClient, UpdateOne

                # final feature list (must match spark_ml_training.feature_cols)
                final_feature_cols = [
                    "west_yusho",
                    "west_makuuchiWins",
                    "west_Makuuchi_basho",
                    "east_yusho",
                    "east_makuuchiWins",
                    "east_Makuuchi_basho",
                    "west_order",
                    "east_order",
                    "east_years_active",
                    "west_years_active",
                    "rolling_winloss_west",
                    "rolling_winloss_east",
                    "west_winRate",
                    "east_winRate",
                    "west_makuuchiWinRate",
                    "east_makuuchiWinRate",
                    "height_difference",
                    "weight_difference",
                    "west_kimarite_entropy",
                    "east_kimarite_entropy",
                    "west_specialist_oshi",
                    "east_specialist_oshi",
                    "west_specialist_yotsu",
                    "east_specialist_yotsu",
                    "west_specialist_other",
                    "east_specialist_other",
                    "west_winrate_vs_opponent_specialist",
                    "east_winrate_vs_opponent_specialist",
                ]

                # Load cleaned snapshots from S3 and preview
                cleaned_path = "s3a://ryans-sumo-bucket/gold/cleaned_data/"
                print(f"Reading cleaned snapshots from {cleaned_path}")
                main_all = spark.read.option("mode", "PERMISSIVE").option("recursiveFileLookup", "true").parquet(cleaned_path)
                print("Preview of cleaned_data (main_all):")
                try:
                    main_all.show(50, truncate=False)
                except Exception:
                    print("Could not show full preview of cleaned_data")

                # Recreate training-style features (rolling_winloss, specialist pivot, winrates, entropies)
                main = main_all.withColumn("match_ts", F.col("match_date").cast("timestamp"))

                # Create long form and rolling 6-month window
                west_rows = main.select("match_id", "match_ts", F.col("westId").alias("rikishiId"), F.lit("west").alias("side"), F.col("westWin").cast("int").alias("isWin"))
                east_rows = main.select("match_id", "match_ts", F.col("eastId").alias("rikishiId"), F.lit("east").alias("side"), (1 - F.col("westWin").cast("int")).alias("isWin"))

                long_df = west_rows.unionByName(east_rows).withColumn("ts", F.col("match_ts").cast("long"))
                # 6-month rolling window (~183 days)
                six_months_sec = 183 * 24 * 3600
                w6 = Window.partitionBy("rikishiId").orderBy("ts").rangeBetween(-six_months_sec, 0)
                rolled_long = (
                    long_df.withColumn("wins_6m", F.sum("isWin").over(w6)).withColumn("games_6m", F.count(F.lit(1)).over(w6)).withColumn("rolling_winloss", F.round(F.col("wins_6m") / F.col("games_6m"), 2))
                )
                rolled_wide = (
                    rolled_long.groupBy("match_id").agg(
                        F.first(F.when(F.col("side") == "west", F.col("rolling_winloss")), ignorenulls=True).alias("rolling_winloss_west"),
                        F.first(F.when(F.col("side") == "east", F.col("rolling_winloss")), ignorenulls=True).alias("rolling_winloss_east"),
                    )
                )

                main = main.join(rolled_wide, on="match_id", how="left")
                # Replace null rolling win/loss with 0 for stability (no recent history -> neutral)
                main = main.withColumn("rolling_winloss_west", F.coalesce(F.col("rolling_winloss_west"), F.lit(0.0))).withColumn(
                    "rolling_winloss_east", F.coalesce(F.col("rolling_winloss_east"), F.lit(0.0))
                )

                # Impute heights/weights/ages similar to training
                from pyspark.ml.feature import Imputer
                imputer = Imputer(inputCols=["west_height", "west_weight", "west_age", "east_height", "east_weight", "east_age"], outputCols=["west_height", "west_weight", "west_age", "east_height", "east_weight", "east_age"]).setStrategy("mean")
                main = imputer.fit(main).transform(main)

                main = main.replace("", "NA", subset=["kimarite"]) if "kimarite" in main.columns else main

                def safe_ratio(numer, denom, default=F.lit(None)):
                    return F.when(denom.isNull() | (denom == 0), default).otherwise(numer / denom)

                main = (
                    main.withColumn("west_winRate", safe_ratio(F.col("west_totalWins").cast("double"), F.col("west_totalMatches").cast("double")))
                    .withColumn("east_winRate", safe_ratio(F.col("east_totalWins").cast("double"), F.col("east_totalMatches").cast("double")))
                    .withColumn("west_makuuchiWinRate", safe_ratio(F.col("west_makuuchiWins").cast("double"), F.col("west_Makuuchi_basho").cast("double")))
                    .withColumn("east_makuuchiWinRate", safe_ratio(F.col("east_makuuchiWins").cast("double"), F.col("east_Makuuchi_basho").cast("double")))
                )

                # Compute height/weight differences; coalesce to 0.0 when any side missing
                main = main.withColumn(
                    "height_difference",
                    F.coalesce(F.col("west_height") - F.col("east_height"), F.lit(0.0)),
                ).withColumn(
                    "weight_difference",
                    F.coalesce(F.col("west_weight") - F.col("east_weight"), F.lit(0.0)),
                )

                # specialist / kimarite pivot and entropy
                yotsu_list = [
                    "yorikiri",
                    "yoritaoshi",
                    "uwatenage",
                    "shitatenage",
                    "uwatedashinage",
                    "shitatedashinage",
                    "tsuridashi",
                    "tsuriotoshi",
                    "kimedashi",
                    "kimetaoshi",
                    "sotogake",
                    "uchigake",
                    "kakenage",
                    "kubinage",
                    "kotenage",
                    "tottari",
                    "uwatehineri",
                    "shitatehineri",
                    "kainahineri",
                    "ashitori",
                    "susoharai",
                    "susotori",
                ]
                oshi_list = [
                    "oshidashi",
                    "oshitaoshi",
                    "tsukidashi",
                    "tsukiotoshi",
                    "tsukitaoshi",
                    "hatakikomi",
                    "hikiotoshi",
                    "abisetaoshi",
                    "hikkake",
                    "okuridashi",
                    "okuritaoshi",
                    "okurihikiotoshi",
                ]

                def cat_expr(k):
                    return F.when(k.isin(yotsu_list), F.lit("yotsu")).when(k.isin(oshi_list), F.lit("oshi")).otherwise(F.lit("other"))

                main_winners = (
                    main.withColumn("winnerId", F.when(F.col("westWin") == 1, F.col("westId")).otherwise(F.col("eastId")))
                    .withColumn("kimarite_norm", F.when(F.col("kimarite").isNull() | (F.col("kimarite") == F.lit("NA")), F.lit(None)).otherwise(F.col("kimarite")))
                    .filter(F.col("kimarite_norm").isNotNull())
                    .withColumn("category", cat_expr(F.col("kimarite_norm")))
                )

                counts = main_winners.groupBy("winnerId", "category").count()
                pivot = (
                    counts.groupBy("winnerId").pivot("category", ["oshi", "yotsu", "other"]).sum("count").na.fill(0).withColumn("total", F.col("oshi") + F.col("yotsu") + F.col("other")).withColumn("oshi_pct", F.when(F.col("total") > 0, F.col("oshi") / F.col("total")).otherwise(F.lit(0.0))).withColumn("yotsu_pct", F.when(F.col("total") > 0, F.col("yotsu") / F.col("total")).otherwise(F.lit(0.0))).withColumn("other_pct", F.when(F.col("total") > 0, F.col("other") / F.col("total")).otherwise(F.lit(0.0)))
                )

                def entropy_expr(o, y, ot):
                    def term(p):
                        return F.when(p > 0, p * F.log(p)).otherwise(F.lit(0.0))

                    return -(term(o) + term(y) + term(ot))

                pivot = pivot.withColumn("kimarite_entropy", entropy_expr(F.col("oshi_pct"), F.col("yotsu_pct"), F.col("other_pct")))

                pivot = pivot.withColumn(
                    "specialist",
                    F.when((F.col("oshi_pct") > F.col("yotsu_pct")) & (F.col("oshi_pct") > F.col("other_pct")), F.lit("oshi")).when((F.col("yotsu_pct") > F.col("oshi_pct")) & (F.col("yotsu_pct") > F.col("other_pct")), F.lit("yotsu")).otherwise(F.lit("other")),
                )

                rikishi_profile = pivot.select(F.col("winnerId").alias("rikishiId"), "specialist", "kimarite_entropy")

                main = (
                    main.join(rikishi_profile.withColumnRenamed("specialist", "west_specialist").withColumnRenamed("kimarite_entropy", "west_kimarite_entropy"), on=main.westId == F.col("rikishiId"), how="left").drop("rikishiId")
                    .join(rikishi_profile.withColumnRenamed("specialist", "east_specialist").withColumnRenamed("kimarite_entropy", "east_kimarite_entropy"), on=main.eastId == F.col("rikishiId"), how="left").drop("rikishiId")
                    .withColumn("west_specialist_oshi", (F.col("west_specialist") == "oshi").cast("int"))
                    .withColumn("west_specialist_yotsu", (F.col("west_specialist") == "yotsu").cast("int"))
                    .withColumn("west_specialist_other", (F.col("west_specialist") == "other").cast("int"))
                    .withColumn("east_specialist_oshi", (F.col("east_specialist") == "oshi").cast("int"))
                    .withColumn("east_specialist_yotsu", (F.col("east_specialist") == "yotsu").cast("int"))
                    .withColumn("east_specialist_other", (F.col("east_specialist") == "other").cast("int"))
                )

                # winrates vs opponent specialist
                west_view = main.select(F.col("westId").alias("rikishiId"), F.col("east_specialist").alias("opponent_specialist"), F.when(F.col("westWin") == 1, 1).otherwise(0).alias("win"))
                east_view = main.select(F.col("eastId").alias("rikishiId"), F.col("west_specialist").alias("opponent_specialist"), F.when(F.col("westWin") == 0, 1).otherwise(0).alias("win"))
                rikishi_vs = west_view.union(east_view)

                winrates = rikishi_vs.groupBy("rikishiId", "opponent_specialist").agg(F.count("*").alias("bouts"), F.sum("win").alias("wins")).withColumn("winrate", F.when(F.col("bouts") > 0, F.col("wins") / F.col("bouts")).otherwise(F.lit(None)))

                winrates_pivot = winrates.groupBy("rikishiId").pivot("opponent_specialist", ["oshi", "yotsu", "other"]).agg(F.first("winrate"))

                west_wr = winrates_pivot.withColumnRenamed("oshi", "west_vs_oshi_winrate").withColumnRenamed("yotsu", "west_vs_yotsu_winrate").withColumnRenamed("other", "west_vs_other_winrate")
                main = main.join(west_wr, main.westId == west_wr.rikishiId, "left").drop(west_wr.rikishiId)
                east_wr = winrates_pivot.withColumnRenamed("oshi", "east_vs_oshi_winrate").withColumnRenamed("yotsu", "east_vs_yotsu_winrate").withColumnRenamed("other", "east_vs_other_winrate")
                main = main.join(east_wr, main.eastId == east_wr.rikishiId, "left").drop(east_wr.rikishiId)

                main = (
                    main.withColumn("west_winrate_vs_opponent_specialist", F.when(F.col("east_specialist") == "oshi", F.col("west_vs_oshi_winrate")).when(F.col("east_specialist") == "yotsu", F.col("west_vs_yotsu_winrate")).when(F.col("east_specialist") == "other", F.col("west_vs_other_winrate")).otherwise(F.lit(None)))
                    .withColumn("east_winrate_vs_opponent_specialist", F.when(F.col("west_specialist") == "oshi", F.col("east_vs_oshi_winrate")).when(F.col("west_specialist") == "yotsu", F.col("east_vs_yotsu_winrate")).when(F.col("west_specialist") == "other", F.col("east_vs_other_winrate")).otherwise(F.lit(None)))
                )

                # fill missing numeric with 0 for snapshot convenience
                main = main.na.fill(value=0)

                # Build latest snapshot per rikishi (unprefixed) then restore prefixed names for join
                west_cols = [c for c in main.columns if c.startswith("west_")]
                east_cols = [c for c in main.columns if c.startswith("east_")]

                west_select = [F.col("westId").alias("rikishiId"), F.col("match_ts").alias("match_ts")]
                for c in west_cols:
                    un = c[len("west_"):]
                    west_select.append(F.col(c).alias(un))
                # Also include per-side rolling_winloss into the per-rikishi rows so snapshots will carry it
                # Note: rolled_wide created columns 'rolling_winloss_west' and 'rolling_winloss_east' on main
                # We'll alias those into the unprefixed 'rolling_winloss' field for each side's snapshot
                west_select = west_select + ([F.col("rolling_winloss_west").alias("rolling_winloss")] if "rolling_winloss_west" in main.columns else [])

                east_select = [F.col("eastId").alias("rikishiId"), F.col("match_ts").alias("match_ts")]
                for c in east_cols:
                    un = c[len("east_"):]
                    east_select.append(F.col(c).alias(un))
                east_select = east_select + ([F.col("rolling_winloss_east").alias("rolling_winloss")] if "rolling_winloss_east" in main.columns else [])

                west_snap = main.select(*west_select)
                east_snap = main.select(*east_select)
                snaps = west_snap.unionByName(east_snap)

                w = Window.partitionBy("rikishiId").orderBy(F.col("match_ts").desc())
                latest = snaps.withColumn("rn", row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

                latest_west = latest.select([F.col("rikishiId").alias("west_rikishiId")] + [F.col(c).alias(f"west_{c}") for c in latest.columns if c not in ("rikishiId", "match_ts")])
                latest_east = latest.select([F.col("rikishiId").alias("east_rikishiId")] + [F.col(c).alias(f"east_{c}") for c in latest.columns if c not in ("rikishiId", "match_ts")])

                # Join latest snapshots to incoming matches
                df_feat = df.join(latest_west, df.westId == latest_west.west_rikishiId, how="left").drop("west_rikishiId")
                df_feat = df_feat.join(latest_east, df_feat.eastId == latest_east.east_rikishiId, how="left").drop("east_rikishiId")

                # Deduplicate incoming webhook rows by id to ensure one row per match
                if "id" in df_feat.columns:
                    try:
                        df_feat = df_feat.dropDuplicates(["id"])
                    except Exception:
                        # If dropDuplicates fails for some reason, continue without throwing
                        pass

                # Ensure height/weight difference columns exist on the feature DF.
                # latest_west/latest_east supply prefixed columns like west_height / east_height
                # Compute differences here so they're available for scoring (coalesce -> 0.0)
                try:
                    df_feat = df_feat.withColumn(
                        "height_difference",
                        F.coalesce(F.col("west_height") - F.col("east_height"), F.lit(0.0)),
                    ).withColumn(
                        "weight_difference",
                        F.coalesce(F.col("west_weight") - F.col("east_weight"), F.lit(0.0)),
                    )
                except Exception:
                    # If the prefixed columns don't exist for some reason, add defaults so feature list is complete
                    for _c in ("height_difference", "weight_difference"):
                        if _c not in df_feat.columns:
                            df_feat = df_feat.withColumn(_c, F.lit(0.0))

                # Propagate per-rikishi rolling_winloss into the feature DF.
                # When snapshots were created we aliased per-side rolling into 'rolling_winloss' so
                # latest_west produced a column 'west_rolling_winloss' and latest_east 'east_rolling_winloss'.
                # Create the model-expected names 'rolling_winloss_west' and 'rolling_winloss_east' from them.
                if "west_rolling_winloss" in df_feat.columns:
                    df_feat = df_feat.withColumn("rolling_winloss_west", F.coalesce(F.col("west_rolling_winloss"), F.lit(0.0)))
                else:
                    df_feat = df_feat.withColumn("rolling_winloss_west", F.lit(0.0))

                if "east_rolling_winloss" in df_feat.columns:
                    df_feat = df_feat.withColumn("rolling_winloss_east", F.coalesce(F.col("east_rolling_winloss"), F.lit(0.0)))
                else:
                    df_feat = df_feat.withColumn("rolling_winloss_east", F.lit(0.0))

                # Determine presence
                rep_w = "west_winRate" if "west_winRate" in df_feat.columns else (next((c for c in df_feat.columns if c.startswith("west_")), None))
                rep_e = "east_winRate" if "east_winRate" in df_feat.columns else (next((c for c in df_feat.columns if c.startswith("east_")), None))
                if rep_w and rep_e:
                    df_feat = df_feat.withColumn("west_has_profile", F.col(rep_w).isNotNull()).withColumn("east_has_profile", F.col(rep_e).isNotNull())
                else:
                    df_feat = df_feat.withColumn("west_has_profile", F.lit(False)).withColumn("east_has_profile", F.lit(False))

                both_present = (F.col("west_has_profile") == True) & (F.col("east_has_profile") == True)
                to_score = df_feat.filter(both_present)

                scored_df = None
                try:
                    score_count = to_score.count()
                except Exception:
                    score_count = 0

                if score_count == 0:
                    print("No rows require model scoring (no matches where both rikishi have profiles)")
                else:
                    print(f"Preparing to score {score_count} match(es) where both rikishi have profiles")
                    # Ensure all final feature columns exist on the DataFrame; add missing as nulls
                    score_input = to_score
                    for c in final_feature_cols:
                        if c not in score_input.columns:
                            score_input = score_input.withColumn(c, F.lit(None))

                    # Select only id + feature cols in the exact order expected by the pipeline
                    score_input = score_input.select(*(["id"] + final_feature_cols))

                    # Impute nulls with 0 for all feature columns (driver-side imputation)
                    score_input = score_input.na.fill(0, subset=final_feature_cols)

                    # Show the exact rows/columns we will pass into the pipeline for inspection
                    try:
                        cols_to_show = ["id"] + final_feature_cols
                        print("Rows being sent to model.transform (sample):")
                        score_input.select(*cols_to_show).show(min(200, score_count), truncate=False)
                    except Exception as _e:
                        print(f"Failed to show score_input preview: {_e}")

                    # Attempt to load PipelineModel on driver and transform
                    driver_model = None
                    try:
                        model_path = "s3a://ryans-sumo-bucket/models/xgboost_model"
                        print(f"Attempting to load PipelineModel on driver from {model_path}")
                        driver_model = _PipelineModel.load(model_path)
                        print("Loaded PipelineModel on driver")
                    except Exception as _e:
                        print(f"Driver failed to load PipelineModel: {_e}; will fall back to executor-side match_predict per-row")

                    if driver_model is not None:
                        try:
                            scored_df = driver_model.transform(score_input).select("id", "prediction")
                            print("Model scored rows on driver")
                        except Exception as _e:
                            print(f"Driver model.transform failed: {_e}")

                if scored_df is not None:
                    preds = scored_df.withColumnRenamed("prediction", "AI_prediction")
                else:
                    preds = spark.createDataFrame([], schema="id string, AI_prediction double")

                df_with_preds = df_feat.join(preds, on="id", how="left")
                df_with_preds = df_with_preds.withColumn("AI_prediction", F.when(F.col("AI_prediction").isNull() & (F.col("west_has_profile") == True) & (F.col("east_has_profile") == False), F.lit(1)).when(F.col("AI_prediction").isNull() & (F.col("west_has_profile") == False) & (F.col("east_has_profile") == True), F.lit(0)).otherwise(F.col("AI_prediction")))

                # Replace df with framed df that includes any driver-side predictions
                df = df_with_preds

                # Build executor partition upsert function that will call match_predict per-row if needed
                def make_upsert_partition(mongo_uri_inner, mongo_db_inner, numeric_id_inner):
                    def upsert_partition(rows_iter):
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

                                # Prefer any precomputed AI_prediction column (from driver vectorized scoring)
                                match_prediction = d.get("AI_prediction", None)

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
                                    "AI_prediction": match_prediction,
                                }
                                key = (division, date_key)
                                groups.setdefault(key, []).append(match_doc)

                            ops = []
                            for (division_k, date_k), matches in groups.items():
                                update_path = f"days.{division_k}.{date_k}"
                                ops.append(UpdateOne({"id": numeric_id_inner}, {"$push": {update_path: {"$each": matches}}}))

                            # Also prepare updates for rikishi_pages.upcoming_match
                            rikishi_coll = db.get_collection("rikishi_pages")
                            rikishi_ops = []
                            for (division_k, date_k), matches in groups.items():
                                for m in matches:
                                    east_id = m.get("east_rikishi_id")
                                    west_id = m.get("west_rikishi_id")
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
                                        "AI_prediction": m.get("AI_prediction"),
                                    }
                                    try:
                                        if east_id not in (None, ""):
                                            rikishi_ops.append(UpdateOne({"id": int(east_id)}, {"$set": {"upcoming_match": match_obj}}))
                                    except Exception:
                                        try:
                                            rikishi_ops.append(UpdateOne({"id": east_id}, {"$set": {"upcoming_match": match_obj}}))
                                        except Exception:
                                            pass
                                    try:
                                        if west_id not in (None, "") and west_id != east_id:
                                            rikishi_ops.append(UpdateOne({"id": int(west_id)}, {"$set": {"upcoming_match": match_obj}}))
                                    except Exception:
                                        try:
                                            if west_id not in (None, "") and west_id != east_id:
                                                rikishi_ops.append(UpdateOne({"id": west_id}, {"$set": {"upcoming_match": match_obj}}))
                                        except Exception:
                                            pass

                            # Executor no longer pushes matches into basho_pages; driver will perform
                            # the final insertion so that AI_prediction values are included.
                            if ops:
                                # skip executor-side bulk write to avoid duplicate inserts
                                pass
                            if rikishi_ops:
                                try:
                                    rikishi_coll.bulk_write(rikishi_ops, ordered=False)
                                except Exception:
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

            # --- DRIVER-SIDE: Update homepage with matches including AI predictions ---
            try:
                from pymongo import MongoClient
                client = MongoClient(mongo_uri)
                db = client.get_database(mongo_db)
                hp_coll = db.get_collection('homepage')
                # Ensure homepage doc exists
                hp_coll.update_one({'_homepage_doc': True}, {'$setOnInsert': {'_homepage_doc': True, 'upcoming_matches': []}}, upsert=True)

                # Collect normalized matches from df (include AI_prediction)
                selected = []
                try:
                    # Prefer columns that exist in the DataFrame
                    wanted = ['match_date', 'matchNo', 'eastShikona', 'westShikona', 'division', 'winnerId', 'kimarite', 'eastId', 'westId', 'AI_prediction']
                    cols = [c for c in wanted if c in df.columns]
                    rows = df.select(*cols).collect()
                    for r in rows:
                        d = r.asDict()
                        try:
                            mobj = {
                                'match_date': d.get('match_date'),
                                'match_number': int(d.get('matchNo')) if d.get('matchNo') not in (None, '') else None,
                                'eastshikona': d.get('eastShikona') or d.get('eastshikona'),
                                'westshikona': d.get('westShikona') or d.get('westshikona'),
                                'division': d.get('division'),
                                'winner': int(d.get('winnerId')) if d.get('winnerId') not in (None, '') else None,
                                'kimarite': d.get('kimarite'),
                                'east_rikishi_id': int(d.get('eastId')) if d.get('eastId') not in (None, '') else None,
                                'west_rikishi_id': int(d.get('westId')) if d.get('westId') not in (None, '') else None,
                                'AI_prediction': float(d.get('AI_prediction')) if d.get('AI_prediction') is not None else None,
                            }
                            selected.append(mobj)
                        except Exception:
                            continue
                except Exception as exc:
                    print(f"Failed to collect matches for homepage update: {exc}")

                # Replace existing upcoming_matches with the canonical set for this webhook.
                # The user asked that we replace any existing contents rather than append.
                try:
                    hp_coll.update_one({'_homepage_doc': True}, {'$set': {'upcoming_matches': selected}})
                    print(f"Replaced upcoming_matches on homepage document with {len(selected)} match(es) (driver)")

                    # Also push matches into basho_pages in nested days -> division -> date
                    try:
                        if numeric_id_for_update is not None:
                            bp_coll = db.get_collection('basho_pages')
                            groups = {}
                            for m in selected:
                                dkey = None
                                try:
                                    if m.get('match_date'):
                                        dkey = m.get('match_date')[:10]
                                except Exception:
                                    dkey = None
                                if not dkey:
                                    continue
                                division = m.get('division') or 'Unknown'
                                groups.setdefault((division, dkey), []).append(m)

                            ops = []
                            from pymongo import UpdateOne as _UpdateOne
                            for (division_k, date_k), matches_list in groups.items():
                                update_path = f"days.{division_k}.{date_k}"
                                ops.append(_UpdateOne({'id': numeric_id_for_update}, {'$push': {update_path: {'$each': matches_list}}}))
                            if ops:
                                try:
                                    bp_coll.bulk_write(ops, ordered=False)
                                    print(f"Pushed {sum(len(v) for v in groups.values())} match(es) into basho_pages.id={numeric_id_for_update} (driver)")
                                except Exception as exc:
                                    print(f"Failed to push matches to basho_pages: {exc}")
                    except Exception as exc:
                        print(f"Failed to prepare basho_pages driver updates: {exc}")
                except Exception as exc:
                    print(f"Failed to push upcoming_matches to homepage: {exc}")

                client.close()
            except Exception as exc:
                print(f"Driver-side homepage update failed: {exc}")

    else:
        print("MONGO_URI not set; skipping write to MongoDB. Sample output:")
        for r in df.take(20):
            print(r)

    spark.stop()


if __name__ == "__main__":
    main()
