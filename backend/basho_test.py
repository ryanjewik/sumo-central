
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
    
    
conn = psycopg2.connect(
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USERNAME"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
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
                WHERE b.id = 200201
                ORDER BY match_date, match_number;
                """)
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


# Save to a JSON file
with open('basho_days_dict.json', 'w', encoding='utf-8') as f:
    json.dump(convert(days_dict), f, ensure_ascii=False, indent=2)
print("Saved to basho_days_dict.json")