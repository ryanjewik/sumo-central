import os
from pathlib import Path

MONGO_URI = os.environ.get('MONGO_URI')
MONGO_DB = os.environ.get('MONGO_DB_NAME')

if not MONGO_URI:
    print('No MONGO_URI found in environment')
    raise SystemExit(1)
if not MONGO_DB:
    print('No MONGO_DB_NAME found in environment')
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
