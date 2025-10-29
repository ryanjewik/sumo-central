import sys
import os
import json as _json
from datetime import datetime, timezone
from airflow.hooks.base import BaseHook


    

    
def process_end_basho(webhook_payload):
    raw = webhook_payload.get("payload_decoded") if isinstance(webhook_payload, dict) else None
    if not raw:
        print("Invalid webhook payload: missing 'payload_decoded'")
        return None

    try:
        bashoId = int(raw.get('date'))
    except Exception:
        print("Invalid or missing 'date' in payload")
        return None
    print(raw)
    yushos = raw.get('yusho') or []

    # Initialize outputs so they are always defined
    makuuchi_yusho = ""
    juryo_yusho = ""
    sandanme_yusho = ""
    makushita_yusho = ""
    jonidan_yusho = ""
    jonokuchi_yusho = ""

    for yusho in yushos:
        print("Processing yusho:", yusho)
        # Some payloads use 'division', others use 'type' (case-insensitive)
        division_raw = yusho.get('type')
        enShikona = yusho.get('shikonaEn')
        jpShikona = yusho.get('shikonaJp')
        combined = f"{enShikona} ({jpShikona})" if (enShikona or jpShikona) else ""

        if 'Makuuchi' in division_raw:
            makuuchi_yusho = combined
        elif 'Juryo' in division_raw:
            juryo_yusho = combined
        elif 'Sandanme' in division_raw:
            sandanme_yusho = combined
        elif 'Makushita' in division_raw:
            makushita_yusho = combined
        elif 'Jonidan' in division_raw:
            jonidan_yusho = combined
        elif 'Jonokuchi' in division_raw:
            jonokuchi_yusho = combined
            
    payload = {
        "basho_id": bashoId,
        "makuuchi_yusho": makuuchi_yusho,
        "juryo_yusho": juryo_yusho,
        "sandanme_yusho": sandanme_yusho,
        "makushita_yusho": makushita_yusho,
        "jonidan_yusho": jonidan_yusho,
        "jonokuchi_yusho": jonokuchi_yusho
        }
    
    #mongo connection
    db_name = os.environ.get("MONGO_DB_NAME")


    try:
        from airflow.providers.mongo.hooks.mongo import MongoHook
    except Exception as e:
        print("Failed to import MongoHook: %s", e)
        raise

    try:
        hook = MongoHook(conn_id="mongo_default")
    except Exception as e:
        print("Failed to create MongoHook: %s", e)
        raise
    # The MongoHook does not expose high-level find_one_and_update in some Airflow versions.
    # Use the underlying pymongo client instead.
    try:
        client = hook.get_conn()
        # Select DB: prefer explicit env var, else use client's default
        if db_name:
            db_obj = client[db_name]
        else:
            try:
                db_obj = client.get_default_database()
            except Exception:
                db_obj = client
        print(payload)
        collection = db_obj.get_collection("basho_pages")
        inserted_obj = collection.find_one_and_update(
            {"id": bashoId},
            {"$set": {
                "basho.makuuchi_yusho": makuuchi_yusho,
                "basho.juryo_yusho": juryo_yusho,
                "basho.sandanme_yusho": sandanme_yusho,
                "basho.makushita_yusho": makushita_yusho,
                "basho.jonidan_yusho": jonidan_yusho,
                "basho.jonokuchi_yusho": jonokuchi_yusho
            }},
            upsert=False
        )
        print("updated basho page:", inserted_obj)
    except Exception as e:
        print("Failed to update basho_pages in MongoDB:", e)
        raise
    

def main():
    if len(sys.argv) > 1:
        webhook_json_arg = sys.argv[1]
    else:
        print("Missing webhook JSON argument")
        sys.exit(2)

    try:
        payload_obj = _json.loads(webhook_json_arg)
    except Exception as e:
        print("Failed to parse webhook JSON from argument: ", e)
        sys.exit(2)

    try:
        process_end_basho(payload_obj)
    except Exception:
        print("process_end_basho failed")
        sys.exit(2)





if __name__ == "__main__":
    main()
