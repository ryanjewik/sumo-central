
#for the homepage
import os, sys, json, time
from dotenv import load_dotenv
load_dotenv()
import datetime
from collections import defaultdict
from collections import Counter
import decimal

try:
    from pyspark.sql import SparkSession
except Exception:
    SparkSession = None


def get_spark_session_for_postgres(app_name: str = "spark_homepage", postgres_jdbc_package: str = "org.postgresql:postgresql:42.6.0"):
    if SparkSession is None:
        raise RuntimeError("pyspark not available in this environment")

    builder = SparkSession.builder.appName(app_name)
    builder = builder.config("spark.jars.packages", postgres_jdbc_package)
    builder = builder.config("spark.pyspark.python", "python3").config("spark.pyspark.driver.python", "python3")
    spark = builder.getOrCreate()
    return spark


def spark_read_postgres_table(spark, jdbc_url: str, table: str, user: str = None, password: str = None, predicates: list = None):
    reader = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", table).option("driver", "org.postgresql.Driver")
    if user is not None:
        reader = reader.option("user", user)
    if password is not None:
        reader = reader.option("password", password)
    if predicates:
        reader = reader.option("partitionColumn", predicates[0])
    return reader.load()

# end Spark helpers -------------------------------------------------------------



#user database connection with retry mechanism
def connect_to_database(max_retries=30, delay=2, postgres_conn_id=None):
    # Enforce using Airflow's PostgresHook (as in postgresNewBasho.py). Do not fallback to env-var-only connection.
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
    except Exception as e:
        raise RuntimeError("PostgresHook from 'airflow.providers.postgres' is required but not available in this environment") from e

    conn_id = postgres_conn_id or os.getenv("POSTGRES_CONN_ID") or os.getenv("POSTGRES_CONN") or "postgres_default"
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        # Use get_conn() to obtain a DBAPI connection (psycopg2 connection in Airflow's default implementation)
        conn = hook.get_conn()
        return conn
    except Exception as e:
        print(f"❌ Error obtaining PostgresHook connection for conn_id={conn_id}: {e}")
        raise
            
def convert_dates(obj):
    if isinstance(obj, dict):
        return {k: convert_dates(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_dates(v) for v in obj]
    elif isinstance(obj, tuple):
        if len(obj) == 1:
            return convert_dates(obj[0])
        return [convert_dates(v) for v in obj]
    elif isinstance(obj, datetime.date):
        return obj.isoformat()
    else:
        return obj
    
def convert_decimals(obj):
    if isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    else:
        return obj
    
    
def order_ranks(rank_mapping):
    # Convert to list of tuples for sorting
    ranks = [(name, value) for name, value in rank_mapping.items()]

    def rank_sort_key(item):
        name, value = item
        # Unranked goes last
        if value == 0:
            return (float('inf'), 1, name)
        # For same value, East is better than West
        east_west = 0 if 'East' in name else 1 if 'West' in name else 2
        return (value, east_west, name)

    # Sort by value, then east/west, then name
    ranks_sorted = sorted(ranks, key=rank_sort_key)

    # Assign order from 1 to N, use rank name as key
    ordered_mapping = {}
    for i, (name, value) in enumerate(ranks_sorted, 1):
        ordered_mapping[name] = {
            'rank_value': value,
            'order': i
        }
    return ordered_mapping
    


def build_homepage_payload(pg_conn=None, postgres_conn_id=None):
    use_spark = False
    spark = None
    spark_created = False
    try:
        if SparkSession is not None:
            try:
                spark = get_spark_session_for_postgres()
                spark_created = True
                use_spark = True
            except Exception:
                spark = None
                use_spark = False
    except Exception:
        spark = None
        use_spark = False

    if use_spark and spark is not None:
        jdbc = os.environ.get("JDBC_URL") or f"jdbc:postgresql://{os.getenv('DB_HOST','localhost')}:{os.getenv('DB_PORT','5432')}/{os.getenv('DB_NAME','sumo')}"
        try:
            print(f"[spark_homepage] JDBC_URL={jdbc}")
            print(f"[spark_homepage] DB_HOST={os.getenv('DB_HOST')}, DB_PORT={os.getenv('DB_PORT')}, DB_NAME={os.getenv('DB_NAME')}")
        except Exception:
            pass
        user = os.environ.get("DB_USERNAME")
        pwd = os.environ.get("DB_PASSWORD")

        def df_to_list(table_or_query):
            reader = spark_read_postgres_table(spark, jdbc, table_or_query, user=user, password=pwd)
            rows = reader.collect()
            return [r.asDict(recursive=True) for r in rows]

        # Read the needed tables/queries into local lists of dicts
        basho_rows = df_to_list("basho")
        rikishi_rank_rows = df_to_list("rikishi_rank_history")
        rikishi_rows = df_to_list("rikishi")
        matches_rows = df_to_list("matches")

        # Reuse original processing logic but operating on Python lists
        # getting the most recent basho
        most_recent_basho = max([r.get("id") for r in basho_rows if r.get("id") is not None])
        year = int(str(most_recent_basho)[:4])
        month = int(str(most_recent_basho)[4:6])
        date = f"{year}-{month:02d}-01"

        # map the rank names and values
        rank_mapping = {r.get("rank_name"): r.get("rank_value") for r in rikishi_rank_rows if r.get("rank_name") is not None}
        ordered_rank_mapping = order_ranks(rank_mapping)

        # getting the top rikishi from the most recent basho
        top_rank_rows = [r for r in rikishi_rank_rows if r.get("rank_value") is not None and 101 <= r.get("rank_value") <= 499 and r.get("rank_date") == date]
        # order by rank_date DESC, rank_value ASC -- emulate by sorting
        top_rank_rows = sorted(top_rank_rows, key=lambda r: (r.get("rank_date"), -int(r.get("rank_value") if r.get("rank_value") is not None else 0)), reverse=True)

        ordered_top_rikishi = {}
        for r in top_rank_rows:
            rikishi_dict = convert_dates(r.copy())
            rank_name = rikishi_dict.get("rank_name")
            order = ordered_rank_mapping.get(rank_name, {}).get("order")
            if order is not None:
                rikishi_id = rikishi_dict.get("rikishi_id")
                # find rikishi details
                details = next((d for d in rikishi_rows if d.get("id") == rikishi_id), None)
                if details:
                    rikishi_details_dict = convert_dates(details.copy())
                    ordered_top_rikishi[str(order)] = rikishi_details_dict

        ordered_top_rikishi = dict(sorted(ordered_top_rikishi.items(), key=lambda x: int(x[0])))

        # need to get the highlight rikishi and his kimarite counts
        top_rikishi = ordered_top_rikishi.get("1")
        kimarite_counts = {}
        if top_rikishi:
            winner_matches = [m for m in matches_rows if m.get("winner") == top_rikishi.get("id")]
            kimarite_list = [m.get("kimarite") for m in winner_matches]
            kimarite_counts = dict(Counter(kimarite_list))

        # getting the most recent day's matches in Makuuchi
        max_basho_id = max([m.get("basho_id") for m in matches_rows if m.get("basho_id") is not None])
        makuuchi_matches = [m for m in matches_rows if m.get("basho_id") == max_basho_id and m.get("division") == "Makuuchi"]
        if makuuchi_matches:
            max_day = max([m.get("day") for m in makuuchi_matches if m.get("day") is not None])
            recent_rows = [m for m in makuuchi_matches if m.get("day") == max_day]
        else:
            recent_rows = []

        recent_matches = {}
        highlighted_match = None
        lowest_avg = float("inf")
        for row in recent_rows:
            match = convert_dates(row.copy())
            match_title = f"{match.get('eastshikona')} ({match.get('east_rank')}) vs. {match.get('westshikona')} ({match.get('west_rank')})"
            east_order = ordered_rank_mapping.get(match.get("east_rank"), {}).get("order", 0)
            west_order = ordered_rank_mapping.get(match.get("west_rank"), {}).get("order", 0)
            match["rank_avg"] = (east_order + west_order) / 2
            recent_matches[match_title] = match
            if match["rank_avg"] < lowest_avg:
                lowest_avg = match["rank_avg"]
                highlighted_match = match

        # Get counts and average rank order for heya and shusshin
        heya_counts = Counter()
        shusshin_counts = Counter()
        heya_ranks = defaultdict(list)
        for r in rikishi_rows:
            heya = r.get("heya")
            shusshin = r.get("shusshin")
            current_rank = r.get("current_rank")
            heya_counts[heya] += 1
            shusshin_counts[shusshin] += 1
            order = ordered_rank_mapping.get(current_rank, {}).get("order")
            if order is not None:
                heya_ranks[heya].append(order)

        heya_avg_rank = {}
        for h, ranks in heya_ranks.items():
            filtered_ranks = [r for r in ranks if r is not None]
            if filtered_ranks:
                heya_avg_rank[h] = sum(filtered_ranks) / len(filtered_ranks)
            else:
                heya_avg_rank[h] = 0

        heya_counts_clean = {str(k) if k is not None else "Unknown": v for k, v in heya_counts.items()}
        shusshin_counts_clean = {str(k) if k is not None else "Unknown": v for k, v in shusshin_counts.items()}

        # kimarite usage for most recent basho
        kimarite_usage = Counter()
        for m in matches_rows:
            if m.get("basho_id") == most_recent_basho:
                kim = m.get("kimarite")
                kimarite_usage[kim] += 1

        # average stats
        weights = [r.get("current_weight") for r in rikishi_rows if r.get("current_weight") is not None]
        heights = [r.get("current_height") for r in rikishi_rows if r.get("current_height") is not None]

        avg_weight = round(sum(weights) / len(weights), 2) if weights else 0.0
        avg_height = round(sum(heights) / len(heights), 2) if heights else 0.0

        avg_stats = {
            "average_weight_kg": avg_weight,
            "average_height_cm": avg_height,
        }

        # makuuchi yusho aggregates
        yusho_holder = next((b for b in basho_rows if b.get("makuuchi_yusho") is not None), None)
        if yusho_holder:
            # find rikishi matching makuuchi_yusho
            r = next((rr for rr in rikishi_rows if rr.get("id") == yusho_holder.get("makuuchi_yusho")), None)
            makuuchi_yusho_avg_height = round(r.get("current_height"), 2) if r and r.get("current_height") is not None else 0.0
            makuuchi_yusho_avg_weight = round(r.get("current_weight"), 2) if r and r.get("current_weight") is not None else 0.0
        else:
            makuuchi_yusho_avg_height = 0.0
            makuuchi_yusho_avg_weight = 0.0

        avg_stats["makuuchi_yusho_avg_height_cm"] = makuuchi_yusho_avg_height
        avg_stats["makuuchi_yusho_avg_weight_kg"] = makuuchi_yusho_avg_weight

        avg_stats = convert_decimals(avg_stats)

        # Find fast climber (past year)
        rank_history = rikishi_rank_rows
        rank_dates = [r.get("rank_date") for r in rank_history if r.get("rank_date") is not None]
        fast_climber_dict = None
        if rank_dates:
            most_recent_date = max(rank_dates)
            one_year_ago = most_recent_date - datetime.timedelta(days=365)
            recent_rows = [row for row in rank_history if row.get("rank_date") and row.get("rank_date") >= one_year_ago]
            rikishi_trends = defaultdict(list)
            for row in recent_rows:
                rikishi_id = row.get("rikishi_id")
                rank_name = row.get("rank_name")
                rank_date = row.get("rank_date")
                rikishi_trends[rikishi_id].append((rank_date, rank_name))

            max_upward = None
            fast_climber_id = None
            for rikishi_id, history in rikishi_trends.items():
                history_sorted = sorted(history, key=lambda x: x[0])
                if len(history_sorted) < 2:
                    continue
                start_rank = history_sorted[0][1]
                end_rank = history_sorted[-1][1]
                start_order = ordered_rank_mapping.get(start_rank, {}).get("order")
                end_order = ordered_rank_mapping.get(end_rank, {}).get("order")
                if start_order is not None and end_order is not None:
                    upward_movement = start_order - end_order
                    if max_upward is None or upward_movement > max_upward:
                        max_upward = upward_movement
                        fast_climber_id = rikishi_id
            if fast_climber_id is not None:
                fr = next((rr for rr in rikishi_rows if rr.get("id") == fast_climber_id), None)
                if fr:
                    fast_climber_dict = convert_dates(fr.copy())
                    fast_climber_dict["upward_movement_past_year"] = max_upward

        if spark_created and spark is not None:
            try:
                spark.stop()
            except Exception:
                pass

        payload = {
            "_homepage_doc": True,
            "most_recent_basho": most_recent_basho,
            "top_rikishi_ordered": ordered_top_rikishi,
            "top_rikishi": top_rikishi,
            "kimarite_counts": kimarite_counts,
            "recent_matches": recent_matches,
            "highlighted_match": highlighted_match,
            "heya_avg_rank": heya_avg_rank,
            "heya_counts": heya_counts_clean,
            "shusshin_counts": shusshin_counts_clean,
            "kimarite_usage_most_recent_basho": dict(kimarite_usage),
            "avg_stats": avg_stats,
            "fast_climber": fast_climber_dict,
        }

        return payload

    

def _connect_mongo_or_hook(mongo_conn_id=None):
    MONGO_CONN_ID = mongo_conn_id or os.getenv("MONGO_CONN_ID") or os.getenv("MONGO_CONN") or None
    try:
        from airflow.providers.mongo.hooks.mongo import MongoHook
        mongo_hook_available = True
    except Exception:
        mongo_hook_available = False
    # If Airflow's MongoHook is available and a conn id is configured, prefer it.
    if mongo_hook_available and MONGO_CONN_ID:
        try:
            mhook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
            return mhook.get_conn()
        except Exception:
            # Fall through to attempt pymongo using MONGO_URI
            pass

    # Fallback: try to use pymongo if available and MONGO_URI is set.
    uri = os.getenv("MONGO_URI")
    if not uri:
        # No connection information available
        return None

    try:
        # Import pymongo lazily so module import doesn't fail when pymongo isn't installed
        from pymongo import MongoClient
        return MongoClient(
            uri,
            maxPoolSize=50,
            serverSelectionTimeoutMS=30000,
            socketTimeoutMS=120000,
            connectTimeoutMS=20000,
        )
    except ImportError:
        # pymongo not installed in this environment
        print("⚠️ pymongo not installed; cannot connect to MongoDB via pymongo. Install 'pymongo' or add Airflow Mongo provider.")
        return None


def run_homepage_job(pg_conn=None, mongo_client=None, postgres_conn_id=None, mongo_conn_id=None, write_to_mongo=True, webhook=None, executor_write=True):
    if isinstance(pg_conn, dict) and webhook is None:
        webhook = pg_conn
        pg_conn = None

    payload = build_homepage_payload(pg_conn=pg_conn, postgres_conn_id=postgres_conn_id)
    try:
        payload["updated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    except Exception:
        payload["updated_at"] = datetime.datetime.utcnow().isoformat() + "Z"

    if write_to_mongo:
        if executor_write and SparkSession is not None:
            try:
                # Ensure a SparkSession exists (prefer helper to reuse configs)
                try:
                    spark = get_spark_session_for_postgres()
                    spark_created_locally = True
                except Exception:
                    spark = SparkSession.builder.appName("homepage_executor_write").getOrCreate()
                    spark_created_locally = False

                sc = spark.sparkContext

                mongo_uri = os.environ.get("MONGO_URI")
                if not mongo_uri:
                    raise RuntimeError("MONGO_URI must be set in the environment for executor-side Mongo writes")

                db_name = os.getenv("MONGO_DB_NAME") or "sumo"
                coll_name = os.getenv("MONGO_COLL_NAME") or "homepage"

                b_uri = sc.broadcast(mongo_uri)
                b_db = sc.broadcast(db_name)
                b_coll = sc.broadcast(coll_name)

                # Broadcast the payload so the executor receives it in the closure
                b_payload = sc.broadcast(payload)

                def _write_partition(rows):
                    # runs on executor
                    try:
                        from pymongo import MongoClient
                    except Exception:
                        # pymongo not available on executor — surface error to driver
                        raise

                    client = MongoClient(b_uri.value)
                    try:
                        db = client.get_database(b_db.value)
                        coll = db[b_coll.value]
                        for _ in rows:
                            doc = dict(b_payload.value)
                            # Ensure marker and avoid accidental _id in payload
                            doc.pop("_id", None)
                            doc["_homepage_doc"] = True
                            coll.replace_one({"_homepage_doc": True}, doc, upsert=True)
                    finally:
                        try:
                            client.close()
                        except Exception:
                            pass

                sc.parallelize([0], 1).foreachPartition(_write_partition)
                return payload
            except Exception as e:
                print(f"⚠️ Executor-side write failed, falling back to driver write: {e}")

        # Driver-side fallback (existing behavior): connect via Airflow MongoHook
        client = mongo_client or _connect_mongo_or_hook(mongo_conn_id=mongo_conn_id)
        if client is None:
            raise RuntimeError("No MongoDB client available: install pymongo or configure Airflow MongoHook/MONGO_URI to enable writing to MongoDB")
        db = client.get_database(os.getenv("MONGO_DB_NAME") or "sumo")
        collection = db["homepage"]

        marker_field = "_homepage_doc"
        payload.pop("_id", None)
        
        existing = collection.find_one({marker_field: True})
        if existing:
            existing_id = existing["_id"]
            replacement = dict(payload)
            replacement[marker_field] = True
            replacement["_id"] = existing_id
            collection.replace_one({"_id": existing_id}, replacement)
            try:
                payload["_id"] = str(existing_id)
            except Exception:
                payload["_id"] = existing_id
        else:
            # No existing doc; insert a new one with the marker
            new_doc = dict(payload)
            new_doc[marker_field] = True
            res = collection.insert_one(new_doc)
            payload["_id"] = str(res.inserted_id)
            
    return payload


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run homepage job (Spark-first)")
    parser.add_argument("--jdbc-url", dest="jdbc_url", help="Full JDBC URL (takes precedence)")
    parser.add_argument("--db-host", dest="db_host", help="Postgres host for JDBC URL construction")
    parser.add_argument("--db-port", dest="db_port", help="Postgres port for JDBC URL construction")
    parser.add_argument("--db-name", dest="db_name", help="Postgres database name for JDBC URL construction")
    parser.add_argument("--db-user", dest="db_user", help="Postgres username for JDBC authentication")
    parser.add_argument("--db-password", dest="db_password", help="Postgres password for JDBC authentication")
    args = parser.parse_args()

    # If a JDBC URL was passed, export it so the rest of the code will use it.
    if args.jdbc_url:
        os.environ["JDBC_URL"] = args.jdbc_url
    else:
        # If individual DB pieces were provided, map them into env vars
        if args.db_host:
            os.environ["DB_HOST"] = args.db_host
        if args.db_port:
            os.environ["DB_PORT"] = args.db_port
        if args.db_name:
            os.environ["DB_NAME"] = args.db_name
        if args.db_user:
            os.environ["DB_USERNAME"] = args.db_user
        if args.db_password:
            os.environ["DB_PASSWORD"] = args.db_password

    # When executed directly, run the job and write to Mongo
    run_homepage_job()
