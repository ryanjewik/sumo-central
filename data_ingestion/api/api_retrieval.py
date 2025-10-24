import requests, json, os
from pathlib import Path
import datetime as dt
import logging
import base64
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import sys



sys.path.append(os.path.join(os.path.dirname(__file__), ".."))  # so "common" is importable
#from common.kafka_utils import publish_event

load_dotenv()
S3_BUCKET = os.getenv("S3_BUCKET")
S3_REGION = os.getenv("AWS_REGION")
S3_PREFIX = os.getenv("S3_PREFIX", "sumo-api-calls/")

# -------- Logging ----------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)

def now_utc_iso() -> str:
    return dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def _safe_json(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), indent=2)

def _boto3_client():
    import boto3
    return boto3.client(
        "s3",
        region_name=S3_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN") or None,
    )

def _s3_enabled():
    return bool(S3_BUCKET and S3_REGION and os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))

def _s3_put_json(doc, key):
    if not _s3_enabled():
        return
    try:
        body = _safe_json(doc).encode("utf-8")
        _boto3_client().put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        print(f"S3 PUT s3://{S3_BUCKET}/{key}")
    except ModuleNotFoundError:
        print("boto3 not installed; skipping S3 upload.")
    except Exception as e:
        print(f"S3 upload failed: {e}")

def _save_to_s3(data, prefix, name):
    stamp = now_utc_iso()
    fname = f"{name}.json"
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"
    s3_key = f"{prefix}{fname}"
    _s3_put_json(data, s3_key)
    
    
    # #publish to kafka
    # try:
    #     publish_event(
    #         {
    #             "source": "api",
    #             "received_at": stamp,
    #             "name": name,       # identifier for the call (e.g., rikishi_123_stats)
    #             "s3_key": s3_key,   # where it landed (if S3 enabled)
    #             "data": data,       # full response (can trim if you want)
    #         },
    #         topic="sumo.api",
    #         key=name,
    #     )   
    # except Exception as e:
    #     logging.exception("Kafka publish failed: %s", e)

# -------- HTTP client with retries ----------
base_url = "https://sumo-api.com/api"

_session = requests.Session()
_retry = Retry(
    total=3,
    connect=3,
    read=3,
    status=3,
    backoff_factor=0.5,
    status_forcelist=(500, 502, 503, 504),
    allowed_methods=frozenset(["GET"]),
)
_adapter = HTTPAdapter(max_retries=_retry)
_session.mount("http://", _adapter)
_session.mount("https://", _adapter)
DEFAULT_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))

def safe_json(res):
    """
    Always return a dict. If API returns a list/null/HTML/error, normalize to {} or {'records': list}.
    """
    try:
        # If response isn't OK, still try to capture body but normalize to {}
        if not getattr(res, "ok", False):
            logging.warning("HTTP %s for %s", getattr(res, "status_code", "?"), getattr(res, "url", "unknown"))
        data = res.json()
        if isinstance(data, dict):
            return data
        if data is None:
            return {}
        if isinstance(data, list):
            return {"records": data}
        return {}
    except Exception as e:
        logging.warning("Error decoding JSON for %s: %s", getattr(res, "url", "unknown"), e)
        return {}

def get_json(path):
    """GET {base_url}{path} with timeout & retries; return dict via safe_json."""
    try:
        res = _session.get(base_url + path, timeout=DEFAULT_TIMEOUT)
        return safe_json(res)
    except Exception as e:
        logging.warning("HTTP error GET %s: %s", path, e)
        return {}

# ---------- Fetch rikishis ----------

# Always stack IDs 1 to 9101, regardless of API response
rikishi_id_stack = list(range(1, 9102))

def upload_rikishi_files():
    for x in rikishi_id_stack:
        rikishi_file = get_json(f"/rikishi/{x}")
        _save_to_s3(rikishi_file, S3_PREFIX + "rikishis", f"rikishi_{x}")

upload_rikishi_files()

# Generate all basho IDs from 195801 to 202509 (months 1,3,5,7,9,11), always 6 digits
def generate_basho_ids(start_year=1958, end_year=2025, end_month=9):
    months = [1, 3, 5, 7, 9, 11]
    basho_ids = []
    for year in range(start_year, end_year + 1):
        for month in months:
            if year == end_year and month > end_month:
                break
            basho_ids.append(int(f"{year}{month:02d}"))
    return basho_ids

basho_ids = generate_basho_ids()

def process_rikishi(rid):
    if not rid:
        return
    # /rikishi/:id/stats
    stats = get_json(f"/rikishi/{rid}/stats")
    _save_to_s3(stats, S3_PREFIX + "rikishi_stats", f"rikishi_{rid}")

    # /rikishi/:id/matches
    matches = get_json(f"/rikishi/{rid}/matches")
    _save_to_s3(matches, S3_PREFIX + "rikishi_matches", f"rikishi_{rid}")

    # /measurements?rikishiId=:id
    measurements = get_json(f"/measurements?rikishiId={rid}")
    _save_to_s3(measurements, S3_PREFIX + "rikishi_measurements", f"rikishi_{rid}_measurements")

    # /ranks?rikishiId=:id
    ranks = get_json(f"/ranks?rikishiId={rid}")
    _save_to_s3(ranks, S3_PREFIX + "rikishi_ranks", f"rikishi_{rid}_ranks")

    # /shikonas?rikishiId=:id
    shikonas = get_json(f"/shikonas?rikishiId={rid}")
    _save_to_s3(shikonas, S3_PREFIX + "rikishi_shikonas", f"rikishi_{rid}_shikonas")

# Parallelize rikishi processing (resilient to individual failures)
if rikishi_id_stack:
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(process_rikishi, rid) for rid in rikishi_id_stack]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                logging.exception("process_rikishi failed: %s", e)

# ---------- Follow-up: basho/banzuke/torikumi ----------
divisions = ['Makuuchi', 'Juryo', 'Makushita', 'Sandanme', 'Jonidan', 'Jonokuchi']

def process_basho(basho_id):
    if not basho_id:
        return
    # /basho/:id
    basho_doc = get_json(f"/basho/{basho_id}")
    _save_to_s3(basho_doc, S3_PREFIX + "basho", f"basho_{basho_id}")

    # /banzuke per division
    for division in divisions:
        banzuke = get_json(f"/basho/{basho_id}/banzuke/{division}")
        _save_to_s3(banzuke, S3_PREFIX + "basho_banzuke", f"basho_{basho_id}_banzuke_{division}")

        # /torikumi per day (1..15)
        for day in range(1, 16):
            torikumi = get_json(f"/basho/{basho_id}/torikumi/{division}/{day}")
            _save_to_s3(torikumi, S3_PREFIX + "basho_torikumi", f"basho_{basho_id}_torikumi_{division}_{day}")

# Only run if we discovered any basho IDs
if basho_ids:
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(process_basho, bid) for bid in list(basho_ids)]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                logging.exception("process_basho failed: %s", e)

logging.info("Done. Rikishi IDs: %d, Basho IDs discovered: %d", len(rikishi_id_stack), len(basho_ids))
