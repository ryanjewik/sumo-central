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
    rikishi_pages = {}

    # Initialize outputs so they are always defined
    makuuchi_yusho = ""
    juryo_yusho = ""
    sandanme_yusho = ""
    makushita_yusho = ""
    jonidan_yusho = ""
    jonokuchi_yusho = ""
    match_date = raw['endDate'][:10] if raw.get('endDate') else None
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
            
        rikishi_pages[int(yusho.get('rikishiId'))] = {
            "yusho": {
                match_date: {
                    "bashoId": bashoId,
                    "location": raw.get("location"),
                    "end_date": match_date,
                    "division_won": f"{division_raw.lower()}_yusho",
                }
            }
        }
    special_prizes = raw.get('specialPrizes') or []
    for prize in special_prizes:
        rikishi_pages[int(prize.get('rikishiId'))] = {
            "special_prize": {
                match_date: {
                    "bashoId": bashoId,
                    "location": raw.get("location"),
                    "end_date": match_date,
                    "prize_name": prize.get('type'),
                }
            }
        }

    print("rikishi_pages:", rikishi_pages)

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
        collection = db_obj.get_collection("rikishi_pages")
        # split up the rikishis into groups based on awards
        for rikishi_id, rikishi_data in rikishi_pages.items():
            # build update payloads ($set and $inc) then call find_one_and_update once
            set_fields = {}
            inc_fields = {}

            if 'yusho' in rikishi_data:
                # rikishi_data['yusho'][match_date] is a dict with the win info
                set_fields[f"division wins.{match_date}"] = rikishi_data['yusho'][match_date]
                inc_fields["rikishi.yusho_count"] = inc_fields.get("rikishi.yusho_count", 0) + 1
                inc_fields["rikishi.basho_count"] = inc_fields.get("rikishi.basho_count", 0) + 1

            if 'special_prize' in rikishi_data:
                set_fields[f"special_prizes.{match_date}"] = rikishi_data['special_prize'][match_date]
                inc_fields["rikishi.sansho_count"] = inc_fields.get("rikishi.sansho_count", 0) + 1
                inc_fields["rikishi.basho_count"] = inc_fields.get("rikishi.basho_count", 0) + 1

            update_payload = {}
            if set_fields:
                update_payload["$set"] = set_fields
            if inc_fields:
                update_payload["$inc"] = inc_fields

            if update_payload:
                try:
                    updated_obj = collection.find_one_and_update(
                        {"id": rikishi_id},
                        update_payload,
                        upsert=True
                    )
                    print("updated rikishi page:", updated_obj)
                except Exception as e:
                    print(f"Failed to update rikishi {rikishi_id}: {e}")
        print("Successfully updated basho_pages and rikishi_pages in MongoDB")
        # Remove the homepage upcoming_matches and any server-derived highlighted
        # match when this basho has ended so consumers don't show stale upcoming UI.
        try:
            hp_coll = db_obj.get_collection("homepage")
            unset_payload = {"$unset": {"upcoming_matches": "", "upcoming_highlighted_match": ""}}
            unset_res = hp_coll.find_one_and_update({"_homepage_doc": True}, unset_payload, upsert=False)
            print("Unset homepage.upcoming_matches and upcoming_highlighted_match; find_one_and_update returned:", unset_res)
        except Exception as e:
            print(f"Failed to unset homepage upcoming keys: {e}")

        # Also remove the dedicated upcoming document (if present) so consumers
        # that rely on the `upcoming: True` doc won't display stale upcoming data
        # after this basho ends.
        try:
            try:
                del_res = db_obj.get_collection("homepage").delete_one({"upcoming": True})
                print("Deleted dedicated upcoming document (upcoming=True); deleted_count=", getattr(del_res, 'deleted_count', None))
            except Exception:
                # if deletion on same collection fails, try an explicit get_collection call
                del_res = db_obj.get_collection("upcoming").delete_one({"upcoming": True})
                print("Deleted dedicated upcoming document from 'upcoming' collection; deleted_count=", getattr(del_res, 'deleted_count', None))
        except Exception as e:
            print(f"Failed to delete dedicated upcoming document: {e}")
    except Exception as e:
        print("Failed to update rikishi and basho_pages in MongoDB:", e)
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
