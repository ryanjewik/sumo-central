
#for the homepage
import psycopg2
import os, sys, json, time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
load_dotenv()
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo import TEXT
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from collections import defaultdict
from pymongo import MongoClient, WriteConcern
from pymongo.errors import AutoReconnect, NetworkTimeout, ConnectionFailure
import time
from collections import Counter
import decimal



#user database connection with retry mechanism
def connect_to_database(max_retries=30, delay=2):
    """
    Attempt to connect to the database with retry logic
    """
    for attempt in range(max_retries):
        try:
            #print(f"Attempting to connect to database (attempt {attempt + 1}/{max_retries}):")

            
            conn = psycopg2.connect(
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USERNAME"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT")
            )
            #print("✅ Database connection successful!")
            return conn
        except Exception as e:
            print(f"❌ Error connecting to the database (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                print(f"⏳ Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"💀 Failed to connect to database after {max_retries} attempts")
                raise e
            
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
    
# Connect to PostgreSQL database
try:
    conn = connect_to_database()
except Exception as e:
    print("❌ Failed to establish database connection:", e)
    conn = None
    
conn = psycopg2.connect(
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USERNAME"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cursor = conn.cursor()

#getting the most recent basho
cursor.execute("""
            SELECT MAX(id) AS most_recent_basho FROM basho
            """)
most_recent_basho = cursor.fetchone()[0]
year = int(str(most_recent_basho)[:4])
month = int(str(most_recent_basho)[4:6])
date = f"{year}-{month:02d}-01"
print(f"Most recent basho: {most_recent_basho} ({date})")
conn.commit()



#map the rank names and values
cursor.execute("""
               SELECT DISTINCT rank_name, rank_value FROM rikishi_rank_history ORDER BY rank_value;
               """)
rows = cursor.fetchall()
rank_mapping = {}
for row in rows:
    rank_name = row[0]
    rank_value = row[1]
    rank_mapping[rank_name] = rank_value

ordered_rank_mapping = order_ranks(rank_mapping)
conn.commit()

#getting the top rikishi from the most recent basho
cursor.execute("""
                SELECT *
                FROM rikishi_rank_history
                WHERE rank_value BETWEEN 101 AND 499
                AND rank_date = %s
                ORDER BY rank_date DESC, rank_value ASC;
                
                """, (date,))
rows = cursor.fetchall()
colnames = [desc[0] for desc in cursor.description]
conn.commit()

# Build a new dict ordered by rank order
ordered_top_rikishi = {}
for row in rows:
    rikishi_dict = dict(zip(colnames, row))
    rikishi_dict = convert_dates(rikishi_dict)
    rank_name = rikishi_dict.get('rank_name')
    order = str(ordered_rank_mapping.get(rank_name, {}).get('order'))
    if order is not None:
        cursor.execute("""
                       SELECT * FROM rikishi WHERE id = %s;
                       """, (rikishi_dict['rikishi_id'],))
        rikishi_details = cursor.fetchone()
        detail_colnames = [desc[0] for desc in cursor.description]
        rikishi_details_dict = dict(zip(detail_colnames, rikishi_details))
        rikishi_details_dict = convert_dates(rikishi_details_dict)
        ordered_top_rikishi[order] = rikishi_details_dict


# Ensure keys are sorted numerically
ordered_top_rikishi = dict(sorted(ordered_top_rikishi.items(), key=lambda x: int(x[0])))


#connect to Mongo database
uri = os.getenv("MONGO_URI")

# 1) ONE global client (thread-safe). Do NOT create one per thread.
client = MongoClient(
    uri,
    maxPoolSize=50,                 # tune pool for your thread count
    serverSelectionTimeoutMS=30000, # 30s
    socketTimeoutMS=120000,         # 120s for large batches
    connectTimeoutMS=20000,
)
db = client["sumo"]                 # your DB
collection = db["homepage"]         # your collection
#collection.insert_one({"top_rikishi": ordered_top_rikishi})
print("inserted top rikishi into MongoDB")

top_rikishi = ordered_top_rikishi['1']


#need to get the highlight rikishi and his kimarite counts
cursor.execute("""
               SELECT DISTINCT basho_id, east_rikishi_id, west_rikishi_id, day, match_number, winner, kimarite FROM matches WHERE winner = %s;
               """, (top_rikishi['id'],))
rows = cursor.fetchall()
colnames = [desc[0] for desc in cursor.description]
conn.commit()
kimarite_counts = []
for row in rows:
    kimarite = dict(zip(colnames, row))
    kimarite_counts.append(kimarite['kimarite'])
kimarite_counts = dict(Counter(kimarite_counts))
#collection.insert_one({"highlight_rikishi": top_rikishi, "kimarite_counts": kimarite_counts})


#getting the most recent day's matches in Makuuchi
cursor.execute("""
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
conn.commit()
recent_matches = {}
highlighted_match = None
lowest_avg = float('inf')
for row in rows:
    match = dict(zip(colnames, row))
    match = convert_dates(match)
    match_title = str(match['eastshikona']) + " (" + str(match['east_rank']) + ") vs. " + str(match['westshikona']) + " (" + str(match['west_rank']) + ")"
    east_order = ordered_rank_mapping.get(match['east_rank'], {}).get('order', 0)
    west_order = ordered_rank_mapping.get(match['west_rank'], {}).get('order', 0)
    match['rank_avg'] = (east_order + west_order) / 2
    recent_matches[match_title] = match
    if match['rank_avg'] < lowest_avg:
        lowest_avg = match['rank_avg']
        highlighted_match = match

#collection.insert_one({"recent_makuuchi_matches": recent_matches, "highlighted_match": highlighted_match})

# Get counts and average rank order for heya and shusshin
cursor.execute("""
    SELECT current_rank, heya, shusshin FROM rikishi;
""")
rows = cursor.fetchall()
colnames = [desc[0] for desc in cursor.description]
conn.commit()

heya_counts = Counter()
shusshin_counts = Counter()
heya_ranks = defaultdict(list)

for row in rows:
    rikishi = dict(zip(colnames, row))
    heya = rikishi['heya']
    shusshin = rikishi['shusshin']
    current_rank = rikishi['current_rank']
    heya_counts[heya] += 1
    shusshin_counts[shusshin] += 1
    order = ordered_rank_mapping.get(current_rank, {}).get('order')
    if order is not None:
        heya_ranks[heya].append(order)

# Calculate average rank for each heya, ignoring None values
heya_avg_rank = {}
for h, ranks in heya_ranks.items():
    filtered_ranks = [r for r in ranks if r is not None]
    if filtered_ranks:
        heya_avg_rank[h] = sum(filtered_ranks) / len(filtered_ranks)
    else:
        heya_avg_rank[h] = 0

heya_counts_clean = {str(k) if k is not None else "Unknown": v for k, v in heya_counts.items()}
shusshin_counts_clean = {str(k) if k is not None else "Unknown": v for k, v in shusshin_counts.items()}

#collection.insert_one({"heya_counts": heya_counts_clean})
#collection.insert_one({"shusshin_counts": shusshin_counts_clean})
collection.insert_one({"heya_avg_rank": heya_avg_rank})

cursor.execute("""
    SELECT DISTINCT basho_id, east_rikishi_id, west_rikishi_id, east_rank, west_rank, eastshikona, westshikona, winner, kimarite, day, match_number, division FROM matches WHERE basho_id = %s;
""", (most_recent_basho,))
rows = cursor.fetchall()
colnames = [desc[0] for desc in cursor.description]
conn.commit()

kimarite_usage = Counter()
for row in rows:
    match = dict(zip(colnames, row))
    kimarite = match['kimarite']
    kimarite_usage[kimarite] += 1

kimarite_usage_dict = dict(kimarite_usage)
#collection.insert_one({"kimarite_usage_most_recent_basho": kimarite_usage_dict})

cursor.execute("""
    SELECT current_weight, current_height FROM rikishi;
""")
rows = cursor.fetchall()
conn.commit()

weights = [row[0] for row in rows if row[0] is not None]
heights = [row[1] for row in rows if row[1] is not None]

avg_weight = round(sum(weights) / len(weights), 2) if weights else 0.0
avg_height = round(sum(heights) / len(heights), 2) if heights else 0.0

avg_stats = {
    "average_weight_kg": avg_weight,
    "average_height_cm": avg_height
}
cursor.execute("""
    SELECT AVG(r.current_height) AS makuuchi_yusho_avg_height, AVG(r.current_weight) AS makuuchi_yusho_avg_weight
    FROM rikishi r
    LEFT JOIN basho b ON b.makuuchi_yusho = r.id;
""")
yusho_row = cursor.fetchone()
conn.commit()

makuuchi_yusho_avg_height = round(yusho_row[0], 2) if yusho_row[0] is not None else 0.0
makuuchi_yusho_avg_weight = round(yusho_row[1], 2) if yusho_row[1] is not None else 0.0

avg_stats["makuuchi_yusho_avg_height_cm"] = makuuchi_yusho_avg_height
avg_stats["makuuchi_yusho_avg_weight_kg"] = makuuchi_yusho_avg_weight



avg_stats = convert_decimals(avg_stats)
#collection.insert_one({"rikishi_average_stats": avg_stats})


# Find the rikishi who has climbed the most in rank order over the past year
cursor.execute("SELECT rikishi_id, rank_name, rank_date FROM rikishi_rank_history;")
rows = cursor.fetchall()
colnames = [desc[0] for desc in cursor.description]
conn.commit()

# Find the most recent rank_date
rank_dates = [row[2] for row in rows if row[2] is not None]
if rank_dates:
    most_recent_date = max(rank_dates)
    one_year_ago = (most_recent_date - datetime.timedelta(days=365))
    # Filter to only last year
    recent_rows = [row for row in rows if row[2] >= one_year_ago]
    # Group by rikishi_id
    rikishi_trends = defaultdict(list)
    for row in recent_rows:
        rikishi_id = row[0]
        rank_name = row[1]
        rank_date = row[2]
        rikishi_trends[rikishi_id].append((rank_date, rank_name))
    # For each rikishi, sort by date and get rank order change
    max_upward = None
    fast_climber_id = None
    for rikishi_id, history in rikishi_trends.items():
        history_sorted = sorted(history, key=lambda x: x[0])
        if len(history_sorted) < 2:
            continue
        start_rank = history_sorted[0][1]
        end_rank = history_sorted[-1][1]
        start_order = ordered_rank_mapping.get(start_rank, {}).get('order')
        end_order = ordered_rank_mapping.get(end_rank, {}).get('order')
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
        fast_climber_dict['upward_movement_past_year'] = max_upward
        collection.insert_one({"fast_climbing_rikishi": fast_climber_dict})


cursor.close()
conn.close()