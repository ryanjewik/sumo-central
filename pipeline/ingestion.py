import os, requests
from dotenv import load_dotenv

load_dotenv()
SECRET = os.getenv("webhook_secret_string")

def subscribe_webhooks(destination_url, name="sumoHooks", subscribe_all=True):
    subs = {"newMatches": True}
    if subscribe_all:
        subs.update({"newBasho": True, "matchResults": True, "endBasho": True})

    payload = {
        "name": name,
        "destination": destination_url,
        "secret": SECRET,
        "subscriptions": subs
    }
    r = requests.post("https://www.sumo-api.com/api/webhook/subscribe", json=payload, timeout=10)
    return r.status_code, r.text

def test_webhook(destination_url, hook_type="newMatches", name="sumoHooks"):
    payload = {
        "name": name,
        "destination": destination_url,
        "secret": SECRET,
        "subscriptions": {hook_type: True}
    }
    url = f"https://www.sumo-api.com/api/webhook/test?type={hook_type}"
    r = requests.post(url, json=payload, timeout=10)
    return r.status_code, r.text
