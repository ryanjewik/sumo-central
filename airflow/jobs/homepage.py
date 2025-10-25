
#for the homepage
import os, sys, json, time
import psycopg2
from dotenv import load_dotenv
load_dotenv()
import datetime
from collections import defaultdict
from collections import Counter
import decimal



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
    
def _connect_postgres_or_hook(postgres_conn_id=None):
    """Return a psycopg2 connection object, preferring Airflow PostgresHook if available."""
    return connect_to_database(postgres_conn_id=postgres_conn_id)


def build_homepage_payload(pg_conn=None, postgres_conn_id=None):
    close_conn = False
    if pg_conn is None:
        pg_conn = _connect_postgres_or_hook(postgres_conn_id=postgres_conn_id)
        close_conn = True

    cursor = pg_conn.cursor()

    # getting the most recent basho
    cursor.execute("SELECT MAX(id) AS most_recent_basho FROM basho")
    most_recent_basho = cursor.fetchone()[0]
    year = int(str(most_recent_basho)[:4])
    month = int(str(most_recent_basho)[4:6])
    date = f"{year}-{month:02d}-01"

    # map the rank names and values
    cursor.execute("SELECT DISTINCT rank_name, rank_value FROM rikishi_rank_history ORDER BY rank_value;")
    rows = cursor.fetchall()
    rank_mapping = {row[0]: row[1] for row in rows}
    ordered_rank_mapping = order_ranks(rank_mapping)

    # getting the top rikishi from the most recent basho
    cursor.execute(
        """
        SELECT *
        FROM rikishi_rank_history
        WHERE rank_value BETWEEN 101 AND 499
        AND rank_date = %s
        ORDER BY rank_date DESC, rank_value ASC;
        """,
        (date,),
    )
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]

    ordered_top_rikishi = {}
    for row in rows:
        rikishi_dict = dict(zip(colnames, row))
        rikishi_dict = convert_dates(rikishi_dict)
        rank_name = rikishi_dict.get("rank_name")
        order = ordered_rank_mapping.get(rank_name, {}).get("order")
        if order is not None:
            cursor.execute("SELECT * FROM rikishi WHERE id = %s;", (rikishi_dict["rikishi_id"],))
            rikishi_details = cursor.fetchone()
            detail_colnames = [desc[0] for desc in cursor.description]
            rikishi_details_dict = dict(zip(detail_colnames, rikishi_details))
            rikishi_details_dict = convert_dates(rikishi_details_dict)
            ordered_top_rikishi[str(order)] = rikishi_details_dict

    ordered_top_rikishi = dict(sorted(ordered_top_rikishi.items(), key=lambda x: int(x[0])))

    # need to get the highlight rikishi and his kimarite counts
    top_rikishi = ordered_top_rikishi.get("1")
    kimarite_counts = {}
    if top_rikishi:
        cursor.execute(
            "SELECT DISTINCT basho_id, east_rikishi_id, west_rikishi_id, day, match_number, winner, kimarite FROM matches WHERE winner = %s;",
            (top_rikishi["id"],),
        )
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        kimarite_list = [dict(zip(colnames, row))["kimarite"] for row in rows]
        from collections import Counter

        kimarite_counts = dict(Counter(kimarite_list))

    # getting the most recent day's matches in Makuuchi
    cursor.execute(
        """
        SELECT DISTINCT basho_id, east_rikishi_id, west_rikishi_id, east_rank, west_rank, eastshikona, westshikona, winner, kimarite, day, match_number, division 
        FROM matches
        WHERE basho_id = (SELECT MAX(basho_id) FROM matches)
        AND division = 'Makuuchi'
        AND day = (
            SELECT MAX(day)
            FROM matches
            WHERE basho_id = (SELECT MAX(basho_id) FROM matches)
                AND division = 'Makuuchi'
        );
        """
    )
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]

    recent_matches = {}
    highlighted_match = None
    lowest_avg = float("inf")
    for row in rows:
        match = dict(zip(colnames, row))
        match = convert_dates(match)
        match_title = f"{match['eastshikona']} ({match['east_rank']}) vs. {match['westshikona']} ({match['west_rank']})"
        east_order = ordered_rank_mapping.get(match["east_rank"], {}).get("order", 0)
        west_order = ordered_rank_mapping.get(match["west_rank"], {}).get("order", 0)
        match["rank_avg"] = (east_order + west_order) / 2
        recent_matches[match_title] = match
        if match["rank_avg"] < lowest_avg:
            lowest_avg = match["rank_avg"]
            highlighted_match = match

    # Get counts and average rank order for heya and shusshin
    cursor.execute("SELECT current_rank, heya, shusshin FROM rikishi;")
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]

    heya_counts = Counter()
    shusshin_counts = Counter()
    heya_ranks = defaultdict(list)

    for row in rows:
        rikishi = dict(zip(colnames, row))
        heya = rikishi["heya"]
        shusshin = rikishi["shusshin"]
        current_rank = rikishi["current_rank"]
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
    cursor.execute(
        "SELECT DISTINCT basho_id, east_rikishi_id, west_rikishi_id, east_rank, west_rank, eastshikona, westshikona, winner, kimarite, day, match_number, division FROM matches WHERE basho_id = %s;",
        (most_recent_basho,),
    )
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    kimarite_usage = Counter()
    for row in rows:
        match = dict(zip(colnames, row))
        kimarite = match["kimarite"]
        kimarite_usage[kimarite] += 1

    # average stats
    cursor.execute("SELECT current_weight, current_height FROM rikishi;")
    rows = cursor.fetchall()

    weights = [row[0] for row in rows if row[0] is not None]
    heights = [row[1] for row in rows if row[1] is not None]

    avg_weight = round(sum(weights) / len(weights), 2) if weights else 0.0
    avg_height = round(sum(heights) / len(heights), 2) if heights else 0.0

    avg_stats = {
        "average_weight_kg": avg_weight,
        "average_height_cm": avg_height,
    }
    cursor.execute(
        """
        SELECT AVG(r.current_height) AS makuuchi_yusho_avg_height, AVG(r.current_weight) AS makuuchi_yusho_avg_weight
        FROM rikishi r
        LEFT JOIN basho b ON b.makuuchi_yusho = r.id;
        """
    )
    yusho_row = cursor.fetchone()

    makuuchi_yusho_avg_height = round(yusho_row[0], 2) if yusho_row[0] is not None else 0.0
    makuuchi_yusho_avg_weight = round(yusho_row[1], 2) if yusho_row[1] is not None else 0.0

    avg_stats["makuuchi_yusho_avg_height_cm"] = makuuchi_yusho_avg_height
    avg_stats["makuuchi_yusho_avg_weight_kg"] = makuuchi_yusho_avg_weight

    avg_stats = convert_decimals(avg_stats)

    # Find fast climber (past year)
    cursor.execute("SELECT rikishi_id, rank_name, rank_date FROM rikishi_rank_history;")
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]

    rank_dates = [row[2] for row in rows if row[2] is not None]
    fast_climber_dict = None
    if rank_dates:
        most_recent_date = max(rank_dates)
        one_year_ago = most_recent_date - datetime.timedelta(days=365)
        recent_rows = [row for row in rows if row[2] >= one_year_ago]
        rikishi_trends = defaultdict(list)
        for row in recent_rows:
            rikishi_id = row[0]
            rank_name = row[1]
            rank_date = row[2]
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
            cursor.execute("SELECT * FROM rikishi WHERE id = %s;", (fast_climber_id,))
            fast_climber_row = cursor.fetchone()
            fast_climber_colnames = [desc[0] for desc in cursor.description]
            fast_climber_dict = dict(zip(fast_climber_colnames, fast_climber_row))
            fast_climber_dict = convert_dates(fast_climber_dict)
            fast_climber_dict["upward_movement_past_year"] = max_upward

    if close_conn:
        cursor.close()
        pg_conn.close()

    payload = {
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


def run_homepage_job(pg_conn=None, mongo_client=None, postgres_conn_id=None, mongo_conn_id=None, write_to_mongo=True, webhook=None):
    if isinstance(pg_conn, dict) and webhook is None:
        webhook = pg_conn
        pg_conn = None

    payload = build_homepage_payload(pg_conn=pg_conn, postgres_conn_id=postgres_conn_id)

    if write_to_mongo:
        client = mongo_client or _connect_mongo_or_hook(mongo_conn_id=mongo_conn_id)
        if client is None:
            raise RuntimeError("No MongoDB client available: install pymongo or configure Airflow MongoHook/MONGO_URI to enable writing to MongoDB")
        db = client.get_database(os.getenv("MONGO_DB_NAME") or "sumo")
        collection = db["homepage"]

        marker_field = "_homepage_doc"

        # Remove any accidental '_id' carried in the payload to avoid conflicts when inserting.
        payload.pop("_id", None)
        
        existing = collection.find_one({marker_field: True})
        if existing:
            existing_id = existing["_id"]
            replacement = dict(payload)
            # keep the marker and ensure the replacement has the same _id so replace_one updates the same doc
            replacement[marker_field] = True
            replacement["_id"] = existing_id
            collection.replace_one({"_id": existing_id}, replacement)
            # return the ObjectId as a string
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
    # When executed directly, run the job and write to Mongo
    run_homepage_job()
