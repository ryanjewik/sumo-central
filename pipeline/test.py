import os, requests
from dotenv import load_dotenv
load_dotenv()

SECRET = os.getenv("webhook_secret_string")
DEST   = os.getenv("DEST_URL")

payload = {
  "name": "sumoHooks-test",
  "destination": DEST,
  "secret": SECRET,
  "subscriptions": { "newMatches": True }
}
import hashlib, os
print("secret_fingerprint:", hashlib.sha256(os.getenv("webhook_secret_string","").encode()).hexdigest()[:12])


r = requests.post("https://www.sumo-api.com/api/webhook/test?type=newMatches",
                  json=payload, timeout=15)
print("Test:", r.status_code, r.text)
