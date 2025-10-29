import sys
import os
import json as _json
import logging
import traceback
from datetime import datetime, timezone
from airflow.hooks.base import BaseHook


    
    

def process_end_basho(webhook_payload):
    raw = webhook_payload.get("payload_decoded") if isinstance(webhook_payload, dict) else None
    bashoId = int(raw.get('date'))
    yushos = raw.get('yusho')
    yusho_rikishi_ids = []
    for yusho in yushos:
        yusho_rikishi_ids.append(yusho.get('rikishiId'))
        if yusho.get('division') == 'makuuchi':
            enShikona = yusho.get('shikonaEn')
            jpShikona = yusho.get('shikonaJp')
            makuuchi_yusho = f"{enShikona} ({jpShikona})"
        elif yusho.get('division') == 'juryo':
            enShikona = yusho.get('shikonaEn')
            jpShikona = yusho.get('shikonaJp')
            juryo_yusho = f"{enShikona} ({jpShikona})"
        elif yusho.get('division') == 'sandanme':
            enShikona = yusho.get('shikonaEn')
            jpShikona = yusho.get('shikonaJp')
            sandanme_yusho = f"{enShikona} ({jpShikona})"
        elif yusho.get('division') == 'makushita':
            enShikona = yusho.get('shikonaEn')
            jpShikona = yusho.get('shikonaJp')
            makushita_yusho = f"{enShikona} ({jpShikona})"
        elif yusho.get('division') == 'jonidan':
            enShikona = yusho.get('shikonaEn')
            jpShikona = yusho.get('shikonaJp')
            jonidan_yusho = f"{enShikona} ({jpShikona})"
        elif yusho.get('division') == 'jonokuchi':
            enShikona = yusho.get('shikonaEn')
            jpShikona = yusho.get('shikonaJp')
            jonokuchi_yusho = f"{enShikona} ({jpShikona})"
            
    special_prizes = raw.get('specialPrizes')
    special_prizes_rikishi_ids = []
    for prize in special_prizes:
        special_prizes_rikishi_ids.append(prize.get('rikishiId'))
            
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
    
    inserted_obj = hook.find_one_and_update(
        mongo_collection="basho_pages",
        query={"id": bashoId},
        update={"$set": {
            "basho.makuuchi_yusho": makuuchi_yusho,
            "basho.juryo_yusho": juryo_yusho,
            "basho.sandanme_yusho": sandanme_yusho,
            "basho.makushita_yusho": makushita_yusho,
            "basho.jonidan_yusho": jonidan_yusho,
            "basho.jonokuchi_yusho": jonokuchi_yusho
        }},
        mongo_db=db_name
    )
    print("update basho page:", inserted_obj)
    

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
