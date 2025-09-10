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
cursor.execute("""
            SELECT MAX(id) AS most_recent_basho FROM basho
            """)
most_recent_basho = cursor.fetchone()[0]
year = int(str(most_recent_basho)[:4])
month = int(str(most_recent_basho)[4:6])
date = f"{year}-{month:02d}-01"
print(f"Most recent basho: {most_recent_basho} ({date})")
conn.commit()
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
if rows:
    top_rikishi_dict = dict(zip(colnames, rows[0]))
    top_rikishi_dict = convert_dates(top_rikishi_dict)
    print(top_rikishi_dict)