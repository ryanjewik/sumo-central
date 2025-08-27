import os, time, requests
from dotenv import load_dotenv
load_dotenv()

SECRET = os.getenv("webhook_secret_string")
DEST   = os.getenv("DEST_URL")  # e.g. https://<your-ngrok>.ngrok-free.app/sumo-webhook
name   = f"sumoHooks-{int(time.time())}"

payload = {
  "name": name,
  "destination": DEST,
  "secret": SECRET,
  "subscriptions": { "newMatches": True }  # add others if you want
}
import hashlib, os
print("secret_fingerprint:", hashlib.sha256(os.getenv("webhook_secret_string","").encode()).hexdigest()[:12])


r = requests.post("https://www.sumo-api.com/api/webhook/subscribe", json=payload, timeout=15)
print("Subscribe:", r.status_code, r.text)
