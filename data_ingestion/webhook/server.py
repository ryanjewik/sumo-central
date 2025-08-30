import base64
import datetime as dt
import hashlib
import hmac
import json
import logging
import os, sys
import threading
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from flask import Flask, request, make_response
import requests
from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))  # so "common" is importable
from common.kafka_utils import publish_event


# ---------- Config / env ----------
load_dotenv()

SUMO_BASE = os.getenv("SUMO_BASE", "https://sumo-api.com").rstrip("/")
DEST_URL = os.getenv("DEST_URL")  # e.g. https://<ngrok>.ngrok-free.app/sumo-webhook-rj1
SUMO_SECRET = os.getenv("SUMO_SECRET")
SUMO_NAME = os.getenv("SUMO_NAME", f"sumoWebhook_{int(dt.datetime.utcnow().timestamp())}")

# S3 config (optional)
S3_BUCKET = os.getenv("S3_BUCKET")           # e.g. ryans-sumo-bucket
S3_REGION = os.getenv("AWS_REGION")           # e.g. us-west-2
S3_PREFIX = os.getenv("S3_PREFIX", "sumo/")  # folder/prefix in bucket; can be ''

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# What we’ll subscribe/test:
EVENT_TYPES = ["newMatches", "newBasho", "matchResults", "endBasho"]

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("server")

# ---------- Flask ----------
app = Flask(__name__)

# ---------- Helpers ----------

def now_utc_iso() -> str:
    return dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def _hmac_hex(secret: str, message: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()

def _full_url_for_signature() -> str:
    """
    Per Sumo docs, signature = HMAC_SHA256(secret, url + body), where url is the exact
    DESTINATION URL subscribed (including scheme/host/path). We'll use DEST_URL as-is.
    https://www.sumo-api.com/webhooks  (Webhooks Guide)  -> HMAC example. 
    """
    if not DEST_URL:
        raise RuntimeError("DEST_URL not set")
    return DEST_URL

def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), indent=2)

def _boto3_client(): #boto automatically finds credentials in the environment file
    # Lazy import so the app can run without boto3 if S3 isn’t used
    import boto3  # type: ignore
    return boto3.client(
        "s3",
        region_name=S3_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN") or None,
    )

def _s3_enabled() -> bool:
    return bool(S3_BUCKET and S3_REGION and os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))

def _s3_put_json(doc: Dict[str, Any], key: str) -> None:
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
        log.info("S3 PUT s3://%s/%s", S3_BUCKET, key)
    except ModuleNotFoundError:
        # boto3 not installed: skip
        log.warning("boto3 not installed; skipping S3 upload.")
    except Exception as e:
        log.exception("S3 upload failed: %s", e)

def _write_local(doc: Dict[str, Any], filepath: Path) -> None:
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.write_text(_safe_json(doc), encoding="utf-8")
    log.info("Wrote %s", filepath)

def _save_record(event_type: str, record: Dict[str, Any]) -> None:
    # Local path
    stamp = record.get("received_at", now_utc_iso())
    fname = f"{stamp}_{event_type}.json"
    local_path = DATA_DIR / event_type / fname
    _write_local(record, local_path)

    # S3 key
    if S3_PREFIX and not S3_PREFIX.endswith("/"):
        prefix = S3_PREFIX + "/"
    else:
        prefix = S3_PREFIX or ""
    s3_key = f"{prefix}{event_type}/{fname}"
    _s3_put_json(record, s3_key)

def _decode_payload_field(payload_b64: str) -> Optional[Any]:
    try:
        raw = base64.b64decode(payload_b64, validate=True)
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None

def _verify_and_build_record(body_bytes: bytes, headers: Dict[str, str]) -> Dict[str, Any]:
    """
    - Verifies signature (if SUMO_SECRET set)
    - Returns normalized record for saving
    Structure:
      {
        "received_at": "<UTC iso stamp>",
        "verified": True/False,
        "type": "<event_type or 'unknown'>",
        "headers": {...},
        "raw": { "type": ..., "payload": "<base64 string>" },
        "payload_decoded": { ... } or None
      }
    """
    received_at = now_utc_iso()

    # Parse body JSON safely
    raw_json = {}
    try:
        raw_json = json.loads(body_bytes.decode("utf-8"))
    except Exception:
        raw_json = {}

    event_type = raw_json.get("type") or "unknown"
    payload_b64 = raw_json.get("payload")

    # Signature verification (per docs: HMAC(secret, url + body)) :contentReference[oaicite:1]{index=1}
    provided_sig = headers.get("X-Webhook-Signature") or headers.get("x-webhook-signature") or ""
    expected_url = _full_url_for_signature()
    url_plus_body = (expected_url.encode("utf-8") + body_bytes) if SUMO_SECRET else b""
    calc_sig = _hmac_hex(SUMO_SECRET, url_plus_body) if SUMO_SECRET else ""

    verified = bool(SUMO_SECRET and provided_sig and provided_sig.lower() == calc_sig.lower())

    # decode payload (if present)
    decoded = _decode_payload_field(payload_b64) if isinstance(payload_b64, str) else None

    record = {
        "received_at": received_at,
        "verified": verified,
        "type": event_type,
        "headers": headers,
        "raw": raw_json if raw_json else {"raw": body_bytes.decode("utf-8", errors="replace")},
        "payload_decoded": decoded,
    }
    return record

# ---------- Sumo API client helpers ----------

def _subscribe(subs: Dict[str, bool]) -> int:
    url = f"{SUMO_BASE}/api/webhook/subscribe"
    payload = {
        "name": SUMO_NAME,
        "destination": DEST_URL,
        "secret": SUMO_SECRET,
        "subscriptions": subs,
    }
    r = requests.post(url, json=payload, timeout=20)
    log.info("SUBSCRIBE status=%s", r.status_code)
    return r.status_code

def _delete_subscription(name: str, secret: str) -> int:
    url = f"{SUMO_BASE}/api/webhook/subscribe"
    payload = {"name": name, "secret": secret}
    r = requests.delete(url, json=payload, timeout=20)
    log.info("DELETE[%s] status=%s", name, r.status_code)
    return r.status_code

def _test_webhook(hook_type: str) -> int:
    """
    POST /api/webhook/test?type=<hook_type> with the same body you used to subscribe.
    The docs note the `type` must match a true flag in subscriptions. :contentReference[oaicite:2]{index=2}
    """
    url = f"{SUMO_BASE}/api/webhook/test"
    params = {"type": hook_type}
    payload = {
        "name": SUMO_NAME,
        "destination": DEST_URL,
        "secret": SUMO_SECRET,
        "subscriptions": {hook_type: True},
    }
    r = requests.post(url, params=params, json=payload, timeout=20)
    log.info("TEST[%s] status=%s", hook_type, r.status_code)
    return r.status_code

def _cleanup_existing(prefix: str) -> None:
    """
    Best-effort cleanup: try to delete a few patterned names you may have used during testing.
    If you always reuse SUMO_NAME, you can simply delete that one.
    """
    try:
        _delete_subscription(SUMO_NAME, SUMO_SECRET or "")
    except Exception as e:
        log.warning("Cleanup error (delete current name): %s", e)

    # If you want to nuke older variants you created earlier, add them here:
    variants = []
    for n in variants:
        try:
            _delete_subscription(n, SUMO_SECRET or "")
        except Exception as e:
            log.warning("Cleanup error (%s): %s", n, e)

def _do_startup_flow():
    if not DEST_URL or not SUMO_SECRET:
        log.error("DEST_URL and SUMO_SECRET must be set in .env")
        return

    log.info("Expected destination to verify: %s", DEST_URL)

    # 1) cleanup (best effort)
    _cleanup_existing(SUMO_NAME)

    # 2) subscribe
    subs = {t: True for t in EVENT_TYPES}
    _subscribe(subs)

    # 3) test each hook
    for t in EVENT_TYPES:
        _test_webhook(t)

# ---------- Routes ----------

@app.route("/cleanup", methods=["POST"])
def cleanup_route():
    _cleanup_existing(SUMO_NAME)
    return {"ok": True}, 200

@app.route("/subscribe", methods=["POST"])
def subscribe_route():
    subs = {t: True for t in EVENT_TYPES}
    code = _subscribe(subs)
    # trigger tests right after
    test_results = {t: _test_webhook(t) for t in EVENT_TYPES}
    return {"subscribe_status": code, "tests": test_results, "destination": DEST_URL}, 200

def _normalized_headers() -> Dict[str, str]:
    return {k: v for k, v in request.headers.items()}

def _webhook_common_handler() -> str:
    # Read raw body exactly as sent; do not parse first
    body_bytes = request.get_data(cache=False, as_text=False)
    headers = _normalized_headers()

    record = _verify_and_build_record(body_bytes, headers)
    event_type = record.get("type", "unknown") or "unknown"

    # Save locally & to S3
    try:
        _save_record(event_type, record)
    except Exception as e:
        log.exception("Persist error: %s", e)
        
    # Public to kafka
    try:
        publish_event(record, topic="sumo.webhooks", key=event_type)
        log.info("Kafka publish ok: topic=sumo.webhooks key=%s", event_type)
    except Exception as e:
        log.exception("Kafka publish failed: %s", e)

    # Per docs, acknowledge with 204 No Content. :contentReference[oaicite:3]{index=3}
    return "", 204

# Use the exact path from DEST_URL as your Flask route
if not DEST_URL:
    log.error("DEST_URL must be set before defining webhook route.")
    path = "/sumo-webhook"
else:
    path = urlparse(DEST_URL).path or "/sumo-webhook"

@app.route(path, methods=["POST"])
def webhook_handler():
    return _webhook_common_handler()

@app.route("/health", methods=["GET"])
def health():
    return {"ok": True, "path": path}, 200

# ---------- Main ----------
if __name__ == "__main__":
    # Kick off cleanup + subscribe + test in a background thread so Flask can start immediately.
    threading.Thread(target=_do_startup_flow, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
