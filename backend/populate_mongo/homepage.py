#for the rikishi and basho pages
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
        ordered_top_rikishi[order] = rikishi_dict

# Optionally sort by order (not strictly necessary for dict, but for output)
ordered_top_rikishi = dict(sorted(ordered_top_rikishi.items()))

#print(json.dumps(ordered_top_rikishi, indent=2, ensure_ascii=False))

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
collection.insert_one({"top_rikishi": ordered_top_rikishi})
print("inserted top rikishi into MongoDB")



#need to get the highlight rikishi
cursor.execute("""
               SELECT DISTINCT basho_id, east_rikishi_id, west_rikishi_id, day, match_number, winner, kimarite FROM matches WHERE winner = 19;
               """)


cursor.close()
conn.close()