
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
            
            
    special_prizes_dict = {}
    cursor.execute("""
                SELECT sp.id, sp.basho_id, sp.prize_name, b.location, b.end_date
                FROM special_prizes sp
                LEFT JOIN basho b ON sp.basho_id = b.id
                WHERE rikishi_id = %s;
                """, (rikishi_id,))
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    conn.commit()
    if rows:
        for row in rows:
            single_entry = dict(zip(colnames, row))
            single_entry = convert_dates(single_entry)
            special_prizes_dict[str(single_entry['end_date'])] = single_entry
            
            
    yusho_history = {}
    cursor.execute("""
                SELECT 
                    b.id AS basho_id,
                    b.location,
                    b.end_date,
                    CASE
                        WHEN b.makuuchi_yusho = %s THEN 'makuuchi_yusho'
                        WHEN b.juryo_yusho = %s THEN 'juryo_yusho'
                        WHEN b.sandanme_yusho = %s THEN 'sandanme_yusho'
                        WHEN b.makushita_yusho = %s THEN 'makushita_yusho'
                        WHEN b.jonidan_yusho = %s THEN 'jonidan_yusho'
                        WHEN b.jonokuchi_yusho = %s THEN 'jonokuchi_yusho'
                    END AS division_won
                FROM basho b
                WHERE EXISTS (
                    SELECT 1
                    FROM matches m
                    WHERE m.basho_id = b.id
                    AND (m.west_rikishi_id = %s OR m.east_rikishi_id = %s)
                )
                AND (
                    b.makuuchi_yusho = %s
                OR b.juryo_yusho = %s
                OR b.sandanme_yusho = %s
                OR b.makushita_yusho = %s
                OR b.jonidan_yusho = %s
                OR b.jonokuchi_yusho = %s
                );

                """, (rikishi_id,))
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    conn.commit()
    if rows:
        for row in rows:
            single_entry = dict(zip(colnames, row))
            single_entry = convert_dates(single_entry)
            yusho_history[str(single_entry['end_date'])] = single_entry

    rikishi_page_document = {
        "id": rikishi_id,
        "updated": datetime.datetime.now().isoformat(),
        "rikishi": rikishi_dict,
        "rikishi_measurements_history": rikishi_measurements_history_dict,
        "rikishi_rank_history": rikishi_rank_history_dict,
        "rikishi_shikona_changes": rikishi_shikona_changes_dict,
        "matches": match_dict,
        "special_prizes": special_prizes_dict,
        "division wins": yusho_history
    }

    rikishi_docs.append(rikishi_page_document)
    cursor.close()
    conn.close()

def create_basho_pages(basho_id, basho_docs):
    conn = psycopg2.connect(
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    cursor = conn.cursor()
    cursor.execute("""
                    SELECT 
                        b.id AS basho_id,
                        b.name AS basho_name,
                        b.location,
                        b.start_date,
                        b.end_date,
                        COALESCE(r_makuuchi.shikona, b.makuuchi_yusho::text)   AS makuuchi_yusho,
                        COALESCE(r_juryo.shikona, b.juryo_yusho::text)         AS juryo_yusho,
                        COALESCE(r_sandanme.shikona, b.sandanme_yusho::text)   AS sandanme_yusho,
                        COALESCE(r_makushita.shikona, b.makushita_yusho::text) AS makushita_yusho,
                        COALESCE(r_jonidan.shikona, b.jonidan_yusho::text)     AS jonidan_yusho,
                        COALESCE(r_jonokuchi.shikona, b.jonokuchi_yusho::text) AS jonokuchi_yusho
                    FROM basho b
                    LEFT JOIN rikishi r_makuuchi   ON r_makuuchi.id   = b.makuuchi_yusho
                    LEFT JOIN rikishi r_juryo      ON r_juryo.id      = b.juryo_yusho
                    LEFT JOIN rikishi r_sandanme   ON r_sandanme.id   = b.sandanme_yusho
                    LEFT JOIN rikishi r_makushita  ON r_makushita.id  = b.makushita_yusho
                    LEFT JOIN rikishi r_jonidan    ON r_jonidan.id    = b.jonidan_yusho
                    LEFT JOIN rikishi r_jonokuchi  ON r_jonokuchi.id  = b.jonokuchi_yusho
                   WHERE b.id = %s
                   """, (basho_id,))
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    for row in rows:
        basho_dict = dict(zip(colnames, row))
    conn.commit()
    
    
    
    days_dict = defaultdict(lambda: defaultdict(list))
    cursor = conn.cursor()
    cursor.execute("""
                    SELECT DISTINCT
                        (b.start_date + (m.day - 1) * INTERVAL '1 day') AS match_date,
                        m.match_number,
                        m.eastshikona,
                        m.westshikona,
                        m.division,
                        CASE 
                            WHEN m.winner = m.east_rikishi_id THEN m.eastshikona
                            WHEN m.winner = m.west_rikishi_id THEN m.westshikona
                            ELSE NULL
                        END AS winner,
                        m.kimarite,
                        m.east_rikishi_id,
                        m.west_rikishi_id,
                        m.winner
                    FROM matches m
                    JOIN basho b ON m.basho_id = b.id
                    WHERE b.id = %s
                    ORDER BY match_date, match_number;
                    """, (basho_id,))
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    for row in rows:
        match_dict = dict(zip(colnames, row))
        division = match_dict["division"]
        match_date = match_dict["match_date"].strftime("%Y-%m-%d")
        days_dict[division][match_date].append(match_dict)
    conn.commit()
    cursor.close()
    conn.close()

    # Sort matches by match_number for each division and date
    for division in days_dict:
        for match_date in days_dict[division]:
            days_dict[division][match_date].sort(key=lambda x: x["match_number"])

    def convert(obj):
        if isinstance(obj, dict):
            return {k: convert(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert(v) for v in obj]
        elif hasattr(obj, 'isoformat'):
            return obj.isoformat()
        else:
            return obj
    days_dict = convert(days_dict)
    
    basho_page_document = {
        "id": basho_id,
        "basho": basho_dict,
        "days": days_dict
    }

    basho_docs.append(basho_page_document)
    cursor.close()
    conn.close()

    pass

#connect to Mongo database
uri = os.getenv("MONGO_URI")

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
db = client[os.getenv("MONGO_DB_NAME") or "your_database"]

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# Connect to PostgreSQL database
try:
    conn = connect_to_database()
except Exception as e:
    print("❌ Failed to establish database connection:", e)
    conn = None
#postgres cursor    
cursor = conn.cursor()
#populate rikishi list
cursor.execute("""
               SELECT id
                FROM rikishi
                """)
rikishi_ids = [row[0] for row in cursor.fetchall()]
#populate basho list
cursor.execute("""
               SELECT id
                FROM basho
                """)
basho_ids = [row[0] for row in cursor.fetchall()]
conn.commit()


#use this to run insertMany in mongo
rikishi_docs = []
basho_docs = []

#get rikishi pages
with ThreadPoolExecutor(max_workers=16) as executor:
    futures = [executor.submit(create_rikishi_pages, rikishi_id, rikishi_docs) for rikishi_id in rikishi_ids]
    for _ in tqdm(as_completed(futures), total=len(futures), desc="Processing Rikishi"):
        pass  # The function already appends to rikishi_docs
#get basho pages
with ThreadPoolExecutor(max_workers=16) as executor:
    futures = [executor.submit(create_basho_pages, basho_id, basho_docs) for basho_id in basho_ids]
    for _ in tqdm(as_completed(futures), total=len(futures), desc="Processing Basho"):
        pass  # The function already appends to basho_docs

#create database
collection = db["rikishi_pages"]
collection.insert_many(rikishi_docs)
collection = db["basho_pages"]
collection.insert_many(basho_docs)
print("Documents inserted into MongoDB.")

cursor.close()
conn.close()