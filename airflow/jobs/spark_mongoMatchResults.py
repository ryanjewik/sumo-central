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

        df = df.withColumn("match_number", F.col("matchNo").cast("int"))
        df = df.withColumn("east_rikishi_id", F.col("eastId").cast("int"))
        df = df.withColumn("west_rikishi_id", F.col("westId").cast("int"))
        df = df.withColumn("winner", F.col("winnerId").cast("int"))
        df = df.withColumnRenamed("westRank", "west_rank")
        df = df.withColumnRenamed("eastRank", "east_rank")

    # We'll print schema / rows after we compute match_date (below)

        # --- New: retrieve the basho_pages document for this webhook ---
        # All matches share the same bashoId; pull it from the first match and cast to int
        try:
            if matches and len(matches) > 0:
                first_basho = matches[0]
                basho_id_raw = first_basho.get("bashoId")
                try:
                    basho_id = int(basho_id_raw)
                except Exception:
                    print(f"Could not cast bashoId '{basho_id_raw}' to int")
                    basho_id = None

                if basho_id is not None:
                    try:
                        # use pymongo on the driver to read the basho_pages document
                        from pymongo import MongoClient

                        mongo_uri_read = os.environ.get("MONGO_URI")
                        if not mongo_uri_read:
                            print("MONGO_URI not set; cannot read basho_pages")
                        else:
                            client = MongoClient(mongo_uri_read)
                            db = client[db_name] if db_name else client.get_default_database()
                            basho_doc = db.get_collection("basho_pages").find_one({"id": basho_id})
                            print("basho_pages document for id", basho_id, ":", basho_doc)
                            # Parse basho start_date and prepare for match_date computation
                            basho_start_date_str = None
                            try:
                                basho_obj = basho_doc.get("basho") if isinstance(basho_doc, dict) else None
                                if basho_obj:
                                    # attempt to read a start_date field; accept either string or date-like
                                    sd = basho_obj.get("start_date")
                                    if sd:
                                        # If string is ISO-like, try fromisoformat; fall back to slicing date part
                                        try:
                                            # datetime.fromisoformat supports 'YYYY-MM-DD' and full ISO
                                            sd_dt = datetime.fromisoformat(sd)
                                            basho_start_date_str = sd_dt.date().isoformat()
                                            print("Parsed basho start_date as", basho_start_date_str)
                                        except Exception:
                                            # Try to be forgiving: take first 10 chars which are likely YYYY-MM-DD
                                            print("Could not parse basho start_date:", Exception)
                            except Exception:
                                basho_start_date_str = None
                            # expose basho_start_date_str for DataFrame transform below
                            _basho_start_date = basho_start_date_str
                    except Exception as e:
                        print("Failed to read basho_pages from Mongo:", e)
        except Exception as e:
            print("Error while attempting to determine bashoId:", e)

        # If we have a basho start date and a 'day' column, compute match_date = start_date + (day - 1)
        try:
            from pyspark.sql import functions as F

            if '_basho_start_date' in locals() and _basho_start_date:
                # ensure day exists; cast to int safely
                if 'day' in df.columns:
                    df = df.withColumn('day', F.col('day').cast('int'))
                    # compute date by adding (day - 1) days to the basho start_date
                    df = df.withColumn(
                        'match_date_date',
                        F.date_add(F.to_date(F.lit(_basho_start_date)), F.col('day') - F.lit(1)),
                    )
                    # format as YYYY-MM-DDT00:00:00
                    df = df.withColumn(
                        'match_date',
                        F.concat(F.date_format(F.col('match_date_date'), "yyyy-MM-dd"), F.lit("T00:00:00")),
                    ).drop('match_date_date')
                else:
                    print("No 'day' column in DataFrame; cannot compute match_date")
            else:
                print("No valid basho start date found; skipping match_date computation")
        except Exception as e:
            print("Failed to compute match_date column:", e)
            
        location = basho_obj.get("location")
        start_date = basho_obj.get("start_date")
        df = df.withColumn("location", F.lit(location))
        df = df.withColumn("start_date", F.lit(start_date))
        df = df.withColumn("bashoId", F.col("bashoId").cast("int"))
        df = df.drop("matchNo", "eastId", "westId", "winnerId", "winnerJp", "winnerEn", "eastRank", "westRank")
        
            
        # rikishi insert here because we need these columns
        # --- Insert per-rikishi match entries into `rikishi_pages` ---
        if mongo_uri:
            try:
                from pymongo import MongoClient
                client = MongoClient(mongo_uri)
                db = client[db_name] if db_name else client.get_default_database()

                # Collect rows to the driver (acceptable for typical webhook sizes)
                rows = df.collect()
                py_rows = [r.asDict(recursive=True) for r in rows]

                rikishi_coll = db.get_collection("rikishi_pages")
                for row in py_rows:
                    match_date = row.get("match_date")
                    match_number = row.get("match_number")
                    if not match_date or match_number is None:
                        continue
                    match_key = f"{match_date[:10]}:match_number:{match_number}"

                    # Insert into east rikishi page
                    east_id = row.get("east_rikishi_id")
                    if east_id:
                        try:
                            payload = dict(row)
                            # remove upcoming_match if present
                            payload.pop("upcoming_match", None)
                            payload["rikishi-shikona"] = payload.get("eastshikona") or payload.get("east_shikona") or payload.get("eastShikona")
                            # choose increment keys
                            inc = {"rikishi.matches": 1}
                            try:
                                winner_val = int(row.get("winner")) if row.get("winner") is not None else None
                            except Exception:
                                winner_val = row.get("winner")
                            if winner_val == int(east_id):
                                inc["rikishi.wins"] = 1
                            else:
                                inc["rikishi.losses"] = 1

                            rikishi_coll.update_one(
                                {"id": int(east_id)},
                                {"$set": {f"matches.{match_key}": payload}, "$inc": inc},
                                upsert=True,
                            )
                            print(f"Upserted match {match_key} into rikishi_pages id={east_id} (east)")
                        except Exception as e:
                            print(f"Failed to upsert east rikishi id={east_id}:", e)

                    # Insert into west rikishi page
                    west_id = row.get("west_rikishi_id")
                    if west_id:
                        try:
                            payload = dict(row)
                            # remove upcoming_match if present
                            payload.pop("upcoming_match", None)
                            payload["rikishi-shikona"] = payload.get("westshikona") or payload.get("west_shikona") or payload.get("westShikona")
                            # choose increment keys
                            inc = {"rikishi.matches": 1}
                            try:
                                winner_val = int(row.get("winner")) if row.get("winner") is not None else None
                            except Exception:
                                winner_val = row.get("winner")
                            if winner_val == int(west_id):
                                inc["rikishi.wins"] = 1
                            else:
                                inc["rikishi.losses"] = 1

                            rikishi_coll.update_one(
                                {"id": int(west_id)},
                                {"$set": {f"matches.{match_key}": payload}, "$inc": inc},
                                upsert=True,
                            )
                            print(f"Upserted match {match_key} into rikishi_pages id={west_id} (west)")
                        except Exception as e:
                            print(f"Failed to upsert west rikishi id={west_id}:", e)
            except Exception as e:
                print("Failed to insert into rikishi_pages:", e)
        else:
            print("MONGO_URI not set; skipping rikishi_pages upserts")
        
        
        df = df.drop("bashoId", "day")
        df.printSchema()
        df.show(5, truncate=False)

        # Additionally, partition the rows by `division` and upsert them into the
        # `basho_pages` document under `days.<division>.<match_date>` using pymongo.
        # This is useful when each division's matches are stored as arrays keyed by date.

        if mongo_uri:
            from pymongo import MongoClient

            client = MongoClient(mongo_uri_read)
            db = client[db_name] if db_name else client.get_default_database()

            # Collect rows to the driver (acceptable for typical webhook sizes)
            rows = df.collect()
            py_rows = [r.asDict(recursive=True) for r in rows]

            if basho_id is None:
                print("Could not determine basho_id for upserts; skipping basho_pages upsert")
            else:
                # Partition rows by division and match_date
                partitions = {}
                for row in py_rows:
                    division = row.get("division") or row.get("division_lower") or "Unknown"
                    match_date = row.get("match_date")
                    if not match_date:
                        # skip rows without match_date — should have been computed earlier
                        continue
                    # Build the entry shape expected in basho_pages
                    entry = {
                        "match_date": match_date,
                        "match_number": row.get("match_number"),
                        "eastshikona": row.get("eastshikona") or row.get("east_shikona") or row.get("eastShikona"),
                        "westshikona": row.get("westshikona") or row.get("west_shikona") or row.get("westShikona"),
                        "division": division,
                        "winner": row.get("winner"),
                        "kimarite": row.get("kimarite"),
                        "east_rikishi_id": row.get("east_rikishi_id"),
                        "west_rikishi_id": row.get("west_rikishi_id"),
                    }

                    partitions.setdefault(division, {}).setdefault(match_date, []).append(entry)

                # Upsert each division's date-array into basho_pages
                coll = db.get_collection("basho_pages")
                for division, date_map in partitions.items():
                    for match_date, entries in date_map.items():
                        # Use only the date portion (YYYY-MM-DD) as the document key
                        date_key = match_date[:10] if isinstance(match_date, str) else str(match_date)
                        field_path = f"days.{division}.{date_key}"
                        try:
                            # Replace the array at this path with the new entries (empty then insert semantics)
                            coll.update_one(
                                {"id": basho_id},
                                {"$set": {field_path: entries}},
                                upsert=True,
                            )
                            print(f"Replaced {field_path} with {len(entries)} rows in basho_pages")
                        except Exception as e:
                            print(f"Failed to set into {field_path}:", e)

        else:
            print("MONGO_URI not set; skipping driver-side basho_pages upserts")

        # Show schema and a few rows — useful during development
        df.printSchema()
        df.show(5, truncate=False)

        return df

    except Exception as e:
        # Fail fast: do not silently fallback — raise so Airflow/Spark can report the error
        print("PySpark path failed; raising error:", e)
        raise


def main():
    if len(sys.argv) < 2:
        print("Provide webhook JSON as first arg")
        sys.exit(2)
    arg = sys.argv[1]
    # If the arg is a path to a file, load the file contents (convenience for testing)
    if os.path.exists(arg):
        with open(arg, 'r', encoding='utf-8') as f:
            payload = json.load(f)
    else:
        payload = json.loads(arg)
    res = process_match_results(payload)
    print("Processed result type:", type(res))


if __name__ == "__main__":
    main()
