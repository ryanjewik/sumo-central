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

uri = os.getenv("MONGO_URI")

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
    
    
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
            
try:
    conn = connect_to_database()
except Exception as e:
    print("❌ Failed to establish database connection:", e)
    conn = None
    
    
cursor = conn.cursor()
    
cursor.execute("""
               SELECT id
                FROM rikishi
                """)
rikishi_ids = [row[0] for row in cursor.fetchall()]
conn.commit()
rikishi_docs = []


def create_rikishi_pages(rikishi_id, cursor, rikishi_docs):
    match_dictionary = {}
    cursor.execute("""
            SELECT m.id, m.basho_id, m.east_rikishi_id, m.west_rikishi_id, m.east_rank, m.west_rank, m.eastshikona, m.westshikona, m.winner, m.kimarite, m.day, m.division, b.location, b.start_date
            FROM matches m
            LEFT JOIN basho b ON m.basho_id = b.id
            WHERE m.west_rikishi_id = %s OR m.east_rikishi_id = %s
            """, (rikishi_id, rikishi_id))
    rows = cursor.fetchall()
    #print(len(rows))
    colnames = [desc[0] for desc in cursor.description]
    conn.commit()
    if rows:
        for row in rows:
            match_dict = dict(zip(colnames, row))
            match_dict = convert_dates(match_dict)
            match_dictionary.setdefault(match_dict['id'], []).append(match_dict)
    else:
        rikishi_dict = {}
    # Save the match_dictionary to a JSON file
    with open(f'match_history_{rikishi_id}.json', 'w', encoding='utf-8') as f:
        # Convert keys to strings for JSON compatibility
        json.dump({str(k): v for k, v in match_dictionary.items()}, f, ensure_ascii=False, indent=2)
    print(f"Saved match history for rikishi_id={rikishi_id} to match_history_{rikishi_id}.json")


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

rikishi_id = rikishi_ids[0]


cursor.execute("""
                SELECT *
                FROM rikishi
                WHERE rikishi.id = %s
                """, (rikishi_id,))
rows = cursor.fetchall()
colnames = [desc[0] for desc in cursor.description]
conn.commit()
if rows:
    rikishi_dict = dict(zip(colnames, rows[0]))
    rikishi_dict = convert_dates(rikishi_dict)
    #print("rikishi:", rikishi_dict)
else:
    rikishi_dict = {}


create_rikishi_pages(rikishi_id, cursor, rikishi_docs)