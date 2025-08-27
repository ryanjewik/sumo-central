from flask import Flask, request, abort, Response
from werkzeug.middleware.proxy_fix import ProxyFix
import os, hmac, hashlib, base64
from dotenv import load_dotenv

load_dotenv()
SECRET = os.getenv("webhook_secret_string")  # same var used by subscribe/test
assert SECRET, "Missing webhook_secret_string in .env"

app = Flask(__name__)
import hashlib, os
print("secret_fingerprint:", hashlib.sha256(os.getenv("webhook_secret_string","").encode()).hexdigest()[:12])
print("DEST_URL loaded:", os.getenv("DEST_URL"))

# Honor X-Forwarded-Proto/Host from ngrok so request.url matches the public https URL
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

def canonical_external_url(req) -> bytes:
    """
    Build the exact URL Sumo-API posted to.
    Prefer an explicit DEST_URL (your public ngrok URL + path) if set.
    Otherwise reconstruct from forwarded headers.
    """
    dest = os.getenv("DEST_URL")
    if dest:  # e.g., https://abc123.ngrok-free.app/sumo-webhook
        return dest.encode()
    

    # Reconstruct: scheme + host + path (including query)
    proto = req.headers.get("X-Forwarded-Proto", req.scheme)
    host  = req.headers.get("X-Forwarded-Host", req.host)
    # request.full_path ends with '?' when no query string; trim it
    path  = req.full_path[:-1] if req.full_path.endswith("?") else req.full_path
    return f"{proto}://{host}{path}".encode()



@app.route("/sumo-webhook", methods=["POST"])
def sumo_webhook():
    # Try canonical_external_url(request) + body
    canonical_url = canonical_external_url(request)
    raw3 = hmac.new(SECRET.encode(), canonical_url + body, hashlib.sha256).digest()
    print("canonical_external_url + body hex:", raw3.hex())
    print("canonical_external_url + body base64:", base64.b64encode(raw3).decode())

    def try_match(secret: str, header_sig: str, candidates: list[tuple[str, bytes]]):
        H = header_sig.strip()
        if H.startswith("sha256="):
            H = H.split("=",1)[1]
        H_lc = H.lower()

        for label, msg in candidates:
            d = hmac.new(secret.encode(), msg, hashlib.sha256).digest()
            if hmac.compare_digest(H_lc, d.hex()) or hmac.compare_digest(H, base64.b64encode(d).decode()):
                print(f"✅ MATCH -> {label}")
                return True
        return False



    sig = request.headers.get("X-Webhook-Signature")
    if not sig:
        return abort(400, "Missing signature header")

    body = request.get_data()  # raw bytes, do NOT json.dumps again
    url_bytes = canonical_external_url(request)
    DEST_URL = os.getenv("DEST_URL")  # for debugging

    # Debug prints for signature troubleshooting
    print("--- Incoming webhook debug ---")
    print("Body (hex):", body.hex())
    print("Signature header:", sig)
    raw = hmac.new(SECRET.encode(), body, hashlib.sha256).digest()
    hex_sig = raw.hex()
    b64_sig = base64.b64encode(raw).decode()
    print("Expected hex signature:", hex_sig)
    print("Expected base64 signature:", b64_sig)
    hdr = sig.split("=",1)[1] if sig.startswith("sha256=") else sig

    if not (hmac.compare_digest(hdr.lower(), hex_sig) or hmac.compare_digest(hdr, b64_sig)):
        # fall back to your existing DEST_URL + body logic if you want both while testing
        raw2 = hmac.new(SECRET.encode(), DEST_URL.encode() + body, hashlib.sha256).digest()
        print("DEST_URL + body hex:", raw2.hex())
        print("DEST_URL + body base64:", base64.b64encode(raw2).decode())
        if not (hmac.compare_digest(hdr.lower(), raw2.hex()) or hmac.compare_digest(hdr, base64.b64encode(raw2).decode())):
            print("Signature mismatch …")
            abort(403, "Invalid signature")

        # in the route:
    body = request.get_data()
    path = request.full_path[:-1] if request.full_path.endswith("?") else request.full_path
    candidates = [
        ("DEST_URL + body",          DEST_URL.encode() + body),
        ("path + body",              path.encode() + body),
        ("body only",                body),
        ("body + DEST_URL",          body + DEST_URL.encode()),
        ("DEST_URL only",            DEST_URL.encode()),
    ]
    if not try_match(SECRET, sig, candidates):
        print("❌ no variant matched", {"url": DEST_URL})
        abort(403, "Invalid signature")


    data = request.get_json(silent=True) or {}
    print("✅ Verified webhook:", data.get("type"))
    return Response(status=204)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
