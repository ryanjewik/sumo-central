import sys
import os
import json as _json
import logging
import traceback
from datetime import datetime, timezone
from airflow.hooks.base import BaseHook

# Structured logger for better Airflow task logs
logger = logging.getLogger("mongoNewBasho")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
logger.propagate = False
logger.debug("[mongoNewBasho] module imported")


def _build_payload_from_raw(raw):
    payload = {"basho": None, "id": None, "days": {}}
    if isinstance(raw, dict):
        start_date = raw.get("startDate")[:10] if raw.get("startDate") else None
        end_date = raw.get("endDate")[:10] if raw.get("endDate") else None
        date_val = raw.get("date") or raw.get("bashoId") or raw.get("id")
        try:
            payload_id = int(date_val) if date_val is not None else None
        except Exception:
            payload_id = None

        payload = {
            "id": payload_id,
            "basho": {
                "basho_id": payload_id,
                "basho_name": f"Basho {date_val}" if date_val else None,
                "location": raw.get("location"),
                "start_date": start_date,
                "end_date": end_date,
                "makuuchi_yusho": None,
                "juryo_yusho": None,
                "sandanme_yusho": None,
                "makushita_yusho": None,
                "jonidan_yusho": None,
                "jonokuchi_yusho": None,
            },
            "days": {},
        }
    elif isinstance(raw, list):
        payload = {"id": None, "basho": raw, "days": {}}
        if raw and isinstance(raw[0], dict):
            date_val = raw[0].get("date") or raw[0].get("bashoId")
            try:
                payload["id"] = int(date_val) if date_val is not None else None
            except Exception:
                payload["id"] = None
    
    return payload


def _insert_via_mongohook(payload, mongo_collection="basho_pages"):
    db_name = os.environ.get("MONGO_DB_NAME")


    try:
        from airflow.providers.mongo.hooks.mongo import MongoHook
    except Exception as e:
        logger.exception("Failed to import MongoHook: %s", e)
        raise

    try:
        hook = MongoHook(conn_id="mongo_default")

        insert_result = None
        try:
            if hasattr(hook, "insert_one"):
                try:
                    insert_result = hook.insert_one(mongo_collection, payload, mongo_db=db_name)
                except TypeError:
                    insert_result = hook.insert_one(mongo_collection, payload)
        except Exception as e:
            logger.exception("Failed to insert via MongoHook: %s", e)
            raise
    except Exception as e:
        raise

    # Normalize the insert result: it may be an InsertOneResult or an inserted id
    if insert_result is None:
        logger.error("Insert did not produce a result")
        raise RuntimeError("Insert did not produce a result")

    if hasattr(insert_result, "inserted_id"):
        inserted_id = insert_result.inserted_id
    else:
        inserted_id = insert_result

    # Convert ObjectId to string so Airflow XCom (JSON) can serialize it
    try:
        inserted_id_str = str(inserted_id)
    except Exception:
        inserted_id_str = inserted_id
    return db_name, inserted_id_str




def process_new_basho(webhook: dict):
    raw = webhook.get("payload_decoded") if isinstance(webhook, dict) and "payload_decoded" in webhook else webhook
    payload = _build_payload_from_raw(raw)
    logger.debug("Prepared payload for MongoDB insertion: %s", payload)

    try:
        db_name, inserted_id = _insert_via_mongohook(payload)
        logger.info("Inserted document into %s.basho_pages, inserted_id=%s", db_name, inserted_id)
        return inserted_id
    except Exception as e:
        logger.exception("MongoHook insert failed: %s", e)
        raise

def main():
    if len(sys.argv) > 1:
        webhook_json_arg = sys.argv[1]
    else:
        logger.error("Expected webhook JSON as first argument (from XCom).")
        sys.exit(2)

    try:
        payload_obj = _json.loads(webhook_json_arg)
    except Exception as e:
        logger.exception("Failed to parse webhook JSON from argument: %s", e)
        sys.exit(2)

    try:
        process_new_basho(payload_obj)
    except Exception:
        logger.exception("process_new_basho failed")
        sys.exit(2)


if __name__ == "__main__":
    main()
