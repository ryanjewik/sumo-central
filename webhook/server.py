# server.py — Sumo API webhook server: subscribe, test, verify, decode, save
import os, hmac, hashlib, json, time, base64
from pathlib import Path
from urllib.parse import urlparse
from flask import Flask, request, jsonify, abort
from dotenv import load_dotenv
import requests

# ---- Config -----------------------------------------------------------------
load_dotenv()
SUMO_BASE   = os.getenv("SUMO_BASE", "https://sumo-api.com")
DEST_URL    = os.getenv("DEST_URL")               # e.g. https://<ngrok>/sumo-webhook-rj1
SUMO_SECRET = os.getenv("SUMO_SECRET")            # e.g. ryanhideo
NAME_PREFIX = os.getenv("NAME_PREFIX", "sumoWebhook_rj")
PORT        = int(os.getenv("PORT", "5000"))
TEST_ON_SUB = os.getenv("TEST_ON_SUBSCRIBE", "1") in ("1", "true", "True")

if not DEST_URL or not SUMO_SECRET:
    raise SystemExit("Please set DEST_URL and SUMO_SECRET in your .env")

WEBHOOK_TYPES = ["newMatches", "newBasho", "matchResults", "endBasho"]

# Folders/files
DATA_DIR   = Path("data"); DATA_DIR.mkdir(exist_ok=True)
EVENT_DIR  = Path("events"); EVENT_DIR.mkdir(exist_ok=True)
SUBS_FILE  = DATA_DIR / "subs.json"

# ---- App --------------------------------------------------------------------
app = Flask(__name__)

def _hmac_sha256_hex(key: bytes, msg: bytes) -> str:
    return hmac.new(key, msg, hashlib.sha256).hexdigest()

def _signature_candidates(dest_url: str, body: bytes):
    """Observed working recipes. We’ll try a small matrix and accept the one that matches."""
    u = urlparse(dest_url)
    host_path = (u.netloc + u.path).encode("utf-8")
    return {
        "host+path + body": host_path + body,
        "body only":        body,
        "full https + body": dest_url.encode("utf-8") + body,
        "path only + body": u.path.encode("utf-8") + body,
    }

def verify_signature(req) -> bool:
    """Check X-Webhook-Signature using SUMO_SECRET with observed provider patterns."""
    provided = (req.headers.get("X-Webhook-Signature") or "").strip().lower()
    if not provided:
        return False
    body = req.get_data()  # raw bytes, exactly as sent
    key  = SUMO_SECRET.encode("utf-8")

    for label, msg in _signature_candidates(DEST_URL, body).items():
        calc = _hmac_sha256_hex(key, msg).lower()
        if hmac.compare_digest(calc[:len(provided)], provided):
            app.logger.info(f"[SIG] OK via: {label}")
            return True

    app.logger.warning("[SIG] mismatch. provided=%s candidates=%s",
        provided, {k: v[:16] for k, v in {lab: _hmac_sha256_hex(key, m) for lab, m in _signature_candidates(DEST_URL, body).items()}.items()}
    )
    return False

def _decode_payload_field(payload_value):
    """Decode base64 JSON payload used by Sumo test deliveries."""
    if isinstance(payload_value, str):
        try:
            raw = base64.b64decode(payload_value)
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return payload_value
    return payload_value

def _save_event(kind: str, envelope: dict):
    """Write event json to events/<kind>-<ts>.json with decoded payload."""
    ts = int(time.time())
    out = {
        "received_at": ts,
        "type": envelope.get("type"),
        "headers": dict(request.headers),
        "raw": envelope,  # keep original keys
    }

    # Decode base64 payload if present
    if isinstance(envelope, dict) and "payload" in envelope:
        out["payload_decoded"] = _decode_payload_field(envelope["payload"])

    fname = EVENT_DIR / f"{kind}-{ts}.json"
    with fname.open("w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    return str(fname)

# ------------------- Webhook endpoint ----------------------------------------
@app.post("/sumo-webhook-rj1")
def webhook_rj1():
    # Verify signature first
    if not verify_signature(request):
        abort(401)

    body_json = request.get_json(silent=True)
    if not isinstance(body_json, dict):
        abort(400)

    event_type = body_json.get("type") or "unknown"
    path = _save_event(event_type, body_json)
    return ("", 204)  # success; no printing

# ------------------- Subscribe / Test / Cleanup ------------------------------
def _load_subs():
    if SUBS_FILE.exists():
        try:
            return json.loads(SUBS_FILE.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []

def _save_subs(names):
    SUBS_FILE.write_text(json.dumps(names, indent=2), encoding="utf-8")

def _subscribe_one(name: str):
    payload = {
        "name": name,
        "destination": DEST_URL,
        "secret": SUMO_SECRET,
        "subscriptions": {
            "newMatches": True,
            "newBasho": True,
            "matchResults": True,
            "endBasho": True,
        },
    }
    r = requests.post(f"{SUMO_BASE}/api/webhook/subscribe", json=payload, timeout=20)
    return r.status_code, r.text

def _test_one(kind: str):
    # Provider supports ?type=<kind>
    payload = {
        "name": "__test__",
        "destination": DEST_URL,
        "secret": SUMO_SECRET,
        "subscriptions": {kind: True},
    }
    r = requests.post(f"{SUMO_BASE}/api/webhook/test", params={"type": kind}, json=payload, timeout=20)
    return r.status_code, r.text

@app.post("/subscribe")
def subscribe():
    name = f"{NAME_PREFIX}_{int(time.time())}"
    status, text = _subscribe_one(name)

    results = {"name": name, "subscribe_status": status}
    created = _load_subs()
    if status == 200:
        created.append(name)
        _save_subs(created)

    if TEST_ON_SUB and status == 200:
        tests = {}
        for kind in WEBHOOK_TYPES:
            s, _ = _test_one(kind)
            tests[kind] = s
        results["tests"] = tests

    return jsonify(results), 200

@app.post("/cleanup")
def cleanup():
    names = _load_subs()
    deleted = []
    for n in names:
        try:
            r = requests.post(f"{SUMO_BASE}/api/webhook/delete", json={"name": n}, timeout=15)
            if r.status_code == 200:
                deleted.append(n)
        except Exception:
            pass
    # Clear local registry for the ones we deleted
    remaining = [n for n in names if n not in deleted]
    _save_subs(remaining)
    return jsonify({"deleted": deleted, "remaining": remaining}), 200

# ------------------- Helpers --------------------------------------------------
@app.get("/health")
def health():
    return jsonify({"ok": True, "dest_url": DEST_URL}), 200

@app.get("/last-files")
def last_files():
    # quick helper: list most recent saved events
    files = sorted((p.name for p in EVENT_DIR.glob("*.json")), reverse=True)[:20]
    return jsonify({"recent": files}), 200

# -----------------------------------------------------------------------------
if __name__ == "__main__":
    app.logger.info("Expected destination to verify: %s", DEST_URL)
    app.run(host="0.0.0.0", port=PORT, debug=False)
