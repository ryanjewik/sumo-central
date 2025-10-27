import os
from pathlib import Path

# Read .env file for MONGO_URI if present
env_file = Path(__file__).resolve().parents[0] / '.env'
env = {}
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' in line:
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip().strip('"')

MONGO_URI = os.environ.get('MONGO_URI') or env.get('MONGO_URI')
MONGO_DB = os.environ.get('MONGO_DB_NAME') or env.get('MONGO_DB_NAME') or 'sumo'

if not MONGO_URI:
    print('No MONGO_URI found in environment or .env')
    raise SystemExit(1)

try:
    from pymongo import MongoClient
except Exception as e:
    print('pymongo not installed:', e)
    raise

client = MongoClient(MONGO_URI)
db = client.get_database(MONGO_DB)
coll = db['homepage']
print('Finding homepage doc...')
doc = coll.find_one({'_homepage_doc': True})
print('doc:', doc)
