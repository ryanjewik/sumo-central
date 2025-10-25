"""Process webhook newMatches events and attach upcoming matches to rikishi_pages documents.

This module exposes `process_new_matches(webhook, mongo_client=None, mongo_conn_id=None, write=True)`
which is suitable to call from an Airflow PythonOperator. It prefers Airflow's MongoHook when
available (use env var MONGO_CONN_ID or pass mongo_conn_id), otherwise falls back to pymongo using
MONGO_URI env var.

Behavior:
- Iterates webhook['payload_decoded'] (list of match dicts).
- For each match, finds the east and west rikishi documents in the `rikishi_pages` collection and
  appends the match dict to an `upcoming_matches` array using $push.
- Returns a list of per-rikishi update results for visibility.

The function is import-safe and uses lazy imports so it can be imported by Airflow without
requiring pymongo to be present until execution.
"""
from __future__ import annotations

import os
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

try:
    # allow dotenv in development; ignore if not present
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

logger = logging.getLogger(__name__)


def _connect_mongo_or_hook(mongo_conn_id: Optional[str] = None):
    """Prefer Airflow MongoHook.get_conn(); otherwise return a pymongo.MongoClient from MONGO_URI.

    Returns None if no connection information is available.
    """
    MONGO_CONN_ID = mongo_conn_id or os.getenv("MONGO_CONN_ID") or os.getenv("MONGO_CONN") or None
    try:
        from airflow.providers.mongo.hooks.mongo import MongoHook

        mongo_hook_available = True
    except Exception:
        mongo_hook_available = False

    if mongo_hook_available and MONGO_CONN_ID:
        try:
            mhook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
            return mhook.get_conn()
        except Exception as e:
            logger.warning("Airflow MongoHook available but failed to get_conn(): %s", e)

    # Fallback to pymongo using MONGO_URI
    uri = os.getenv("MONGO_URI")
    if not uri:
        return None

    try:
        from pymongo import MongoClient

        return MongoClient(uri, serverSelectionTimeoutMS=10000)
    except ImportError:
        logger.warning("pymongo not installed; cannot connect to MongoDB via pymongo.")
        return None


def _normalize_rikishi_id(rid: Any):
    """Normalize rikishi id for querying: try int if possible, else keep original."""
    if rid is None:
        return None
    try:
        return int(rid)
    except Exception:
        return rid


def process_new_matches(webhook: Dict[str, Any], mongo_client: Optional[Any] = None, mongo_conn_id: Optional[str] = None, write: bool = True) -> List[Dict[str, Any]]:
    """Process a webhook payload (dict) and attach upcoming matches to rikishi_pages documents.

    Args:
        webhook: The webhook dict (matching the sample `newMatches-*.json`). Must contain
                 a key `payload_decoded` with a list of match objects.
        mongo_client: Optional pymongo.MongoClient (or a DB client returned by Airflow MongoHook).
        mongo_conn_id: Optional Airflow connection id to use with MongoHook if mongo_client not passed.
        write: If False, do not perform writes; instead return the operations that would be performed.

    Returns a list of result dicts describing updates made (or planned) per rikishi id.
    """
    if not isinstance(webhook, dict):
        raise TypeError("webhook must be a dict")

    payload = webhook.get("payload_decoded") or webhook.get("payload")
    if payload is None:
        logger.info("No payload_decoded found in webhook; nothing to do")
        return []

    if not isinstance(payload, list):
        logger.info("payload_decoded is not a list; nothing to do")
        return []

    client = mongo_client or _connect_mongo_or_hook(mongo_conn_id=mongo_conn_id)
    if client is None:
        raise RuntimeError("No MongoDB client available: set MONGO_CONN_ID or MONGO_URI or pass mongo_client")

    db_name = os.getenv("MONGO_DB_NAME") or "sumo"
    db = client.get_database(db_name)
    collection = db.get_collection("rikishi_pages")

    results: List[Dict[str, Any]] = []

    for match in payload:
        # expect match to be a dict with eastId and westId
        if not isinstance(match, dict):
            continue

        east = match.get("eastId")
        west = match.get("westId")

        # --- Update basho_pages collection: push minimal match info into days.<division>.<date> ---
        try:
            basho_id_raw = match.get("bashoId")
            division = match.get("division") or "Unknown"
            day_num = match.get("day")
            # normalize basho id and construct ISO date
            basho_id_str = str(basho_id_raw) if basho_id_raw is not None else None
            # Also attempt to parse an integer form for systems that store id as int
            basho_id_int = None
            try:
                if basho_id_str is not None:
                    basho_id_int = int(basho_id_str)
            except Exception:
                basho_id_int = None
            if basho_id_str and day_num:
                # First try to read the basho document to get its start_date. Try multiple
                # query shapes/types because some documents store the id as an int and others
                # as a string, or nested under `basho.id`.
                basho_coll = db.get_collection("basho_pages")
                basho_doc = None
                found_basho_query = None
                try:
                    # Only search using integer id forms. The DB stores `id` as an int;
                    # we will not search by the string representation here.
                    possible_queries = []
                    if basho_id_int is not None:
                        possible_queries.append({"id": basho_id_int})
                        possible_queries.append({"basho.id": basho_id_int})

                    for q in possible_queries:
                        basho_doc = basho_coll.find_one(q)
                        if basho_doc:
                            found_basho_query = q
                            break
                except Exception:
                    basho_doc = None
                    found_basho_query = None

                match_date_dt = None
                if basho_doc:
                    # The canonical start_date is stored under the nested 'basho' object.
                    start_date_str = None
                    try:
                        if isinstance(basho_doc.get("basho"), dict):
                            start_date_str = basho_doc["basho"].get("start_date")
                    except Exception:
                        start_date_str = None

                    if start_date_str:
                        # parse common YYYY-MM-DD format
                        try:
                            base_dt = datetime.strptime(start_date_str[:10], "%Y-%m-%d")
                            match_date_dt = base_dt + timedelta(days=int(day_num) - 1)
                        except Exception:
                            match_date_dt = None

                # Fallback: derive from bashoId if we couldn't compute from start_date
                if match_date_dt is None:
                    try:
                        year = int(basho_id_str[:4])
                        month = int(basho_id_str[4:6])
                        day = int(day_num)
                        match_date_dt = datetime(year, month, day)
                    except Exception:
                        match_date_dt = None

                if match_date_dt:
                    match_date_iso = match_date_dt.strftime("%Y-%m-%dT00:00:00")
                    date_key = match_date_dt.strftime("%Y-%m-%d")

                    basho_match = {
                        "match_date": match_date_iso,
                        "match_number": match.get("matchNo") or match.get("match_number"),
                        "eastshikona": match.get("eastshikona"),
                        "westshikona": match.get("westshikona"),
                        "east_rikishi_id": _normalize_rikishi_id(match.get("eastId")),
                        "west_rikishi_id": _normalize_rikishi_id(match.get("westId")),
                        "winner": None,
                        "kimarite": None,
                    }

                    # Dot-path for nested days/division/date
                    dot_path = f"days.{division}.{date_key}"

                    try:
                        # If we matched a basho doc using a particular query, prefer that
                        # query when updating so we don't create a duplicate document with
                        # a different id type. Otherwise prefer an integer id when the
                        # basho id is numeric (to match existing docs that store id as int),
                        # falling back to the string id if integer conversion fails.
                        if found_basho_query is not None:
                            update_filter = found_basho_query
                        else:
                            # Use integer-only filter forms (prefer numeric id) so we match
                            # documents that store id as an integer.
                            if basho_id_int is not None:
                                update_filter = {"$or": [{"id": basho_id_int}, {"basho.id": basho_id_int}]}
                            else:
                                # If we can't parse an int, skip updating basho_pages for this match
                                update_filter = None
                        if write and update_filter is not None:
                            res = basho_coll.update_one(update_filter, {"$push": {dot_path: basho_match}}, upsert=True)
                            results.append({"basho_id": basho_id_str, "division": division, "date": date_key, "pushed": True, "matched_count": res.matched_count, "modified_count": res.modified_count, "update_filter": update_filter})
                        else:
                            results.append({"basho_id": basho_id_str, "division": division, "date": date_key, "would_push": basho_match, "update_filter": update_filter})
                    except Exception as e:
                        logger.exception("Failed to update basho_pages for basho %s: %s", basho_id_str, e)
        except Exception:
            # don't block rikishi updates if basho update fails for any reason
            logger.debug("Skipping basho_pages update for match due to parsing error", exc_info=True)

        for rid in (east, west):
            norm_id = _normalize_rikishi_id(rid)
            if norm_id is None:
                continue

            # Build queries to try to find the rikishi page document. We support docs that store
            # the rikishi id in either an `id` field or the Mongo `_id` field (string or int).
            queries = [
                {"id": norm_id},
                {"_id": norm_id},
                {"_id": str(norm_id)},
            ]

            found = None
            used_query = None
            for q in queries:
                found = collection.find_one(q)
                if found:
                    used_query = q
                    break

            upcoming_entry = dict(match)

            if not found:
                # No existing rikishi page found. We'll create a minimal document if write=True,
                # otherwise report that we'd create one.
                if write:
                    new_doc = {"id": norm_id, "upcoming_matches": [upcoming_entry]}
                    res = collection.insert_one(new_doc)
                    results.append({"rikishi_id": norm_id, "created": True, "inserted_id": str(res.inserted_id)})
                else:
                    results.append({"rikishi_id": norm_id, "created": True, "would_insert": {"id": norm_id, "upcoming_matches": [upcoming_entry]}})
                continue

            # Existing doc found: push the upcoming match into upcoming_matches array
            if write:
                res = collection.update_one({"_id": found.get("_id")}, {"$push": {"upcoming_matches": upcoming_entry}})
                results.append({"rikishi_id": norm_id, "matched_query": used_query, "matched": True, "modified_count": res.modified_count})
            else:
                results.append({"rikishi_id": norm_id, "matched_query": used_query, "matched": True, "would_update": {"$push": {"upcoming_matches": upcoming_entry}}})

    return results


if __name__ == "__main__":
    import json
    import sys

    if len(sys.argv) < 2:
        print("Usage: python mongoNewMatches.py <path-to-webhook-json>")
        sys.exit(2)

    path = sys.argv[1]
    with open(path, "r", encoding="utf-8") as fh:
        webhook = json.load(fh)

    out = process_new_matches(webhook, write=False)
    print(json.dumps(out, indent=2))
