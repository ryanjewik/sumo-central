
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
#print(rikishi_ids)
conn.commit()
rikishi_docs = []


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

def create_rikishi_pages(rikishi_id, rikishi_docs):
    conn = psycopg2.connect(
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    cursor = conn.cursor()

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
    else:
        rikishi_dict = {}

    rikishi_measurements_history_dict = {}
    cursor.execute("""
                SELECT *
                FROM rikishi_measurements_history
                WHERE rikishi_id = %s
                """, (rikishi_id,))
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    conn.commit()
    if rows:
        for row in rows:
            single_entry = dict(zip(colnames, row))
            single_entry = convert_dates(single_entry)
            rikishi_measurements_history_dict[str(single_entry['measurement_date'])] = single_entry

    rikishi_rank_history_dict = {}
    cursor.execute("""
                SELECT *
                FROM rikishi_rank_history
                WHERE rikishi_id = %s
                """, (rikishi_id,))
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    conn.commit()
    if rows:
        for row in rows:
            single_entry = dict(zip(colnames, row))
            single_entry = convert_dates(single_entry)
            rikishi_rank_history_dict[str(single_entry['rank_date'])] = single_entry

    rikishi_shikona_changes_dict = {}
    cursor.execute("""
                SELECT *
                FROM rikishi_shikona_changes
                WHERE rikishi_id = %s
                """, (rikishi_id,))
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    conn.commit()
    if rows:
        for row in rows:
            single_change = dict(zip(colnames, row))
            single_change = convert_dates(single_change)
            rikishi_shikona_changes_dict[str(single_change['change_date'])] = single_change

    match_dict = {}
    cursor.execute("""
                SELECT m.id, m.basho_id, m.east_rikishi_id, m.west_rikishi_id, m.east_rank, m.west_rank, m.eastshikona, m.westshikona, m.winner, m.kimarite, m.day, m.division, b.location, b.start_date
                FROM matches m
                LEFT JOIN basho b ON m.basho_id = b.id
                WHERE m.west_rikishi_id = %s OR m.east_rikishi_id = %s
                """, (rikishi_id, rikishi_id))
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    conn.commit()
    if rows:
        for row in rows:
            single_match = dict(zip(colnames, row))
            single_match = convert_dates(single_match)
            if single_match['west_rikishi_id'] == rikishi_id:
                single_match['rikishi-shikona'] = single_match['westshikona']
            else:
                single_match['rikishi-shikona'] = single_match['eastshikona']
            match_dict[str(single_match['id'])] = single_match

    rikishi_page_document = {
        "id": rikishi_id,
        "updated": datetime.datetime.now().isoformat(),
        "rikishi": rikishi_dict,
        "rikishi_measurements_history": rikishi_measurements_history_dict,
        "rikishi_rank_history": rikishi_rank_history_dict,
        "rikishi_shikona_changes": rikishi_shikona_changes_dict,
        "matches": match_dict
    }

    rikishi_docs.append(rikishi_page_document)
    cursor.close()
    conn.close()

with ThreadPoolExecutor(max_workers=16) as executor:
    futures = [executor.submit(create_rikishi_pages, rikishi_id, rikishi_docs) for rikishi_id in rikishi_ids]
    for _ in tqdm(as_completed(futures), total=len(futures), desc="Processing Rikishi"):
        pass  # The function already appends to rikishi_docs
    
#create database
db = client[os.getenv("MONGO_DB_NAME") or "your_database"]
collection = db["rikishi_pages"]
collection.insert_many(rikishi_docs)
print("Documents inserted into MongoDB.")

#create search index
collection.create_index([("rikishi.shikona", TEXT)], name="rikishi_search_index", default_language="english")

#test search
print("test for search ryo")
cursor = collection.find(
    {"$text": {"$search": "ryo"}},
    {"score": {"$meta": "textScore"}}
).sort("score", {"$meta": "textScore"})

for doc in cursor:
    print("Found document:", doc)

cursor.close()
conn.close()