import requests
import datetime
import os

# Basho IDs to fetch
basho_ids = [
    195801, 195803, 195805, 195807, 195809, 195811,
    195901, 195903, 195905, 195907, 195909, 195911
]

# Base API URL
BASE_URL = "https://sumo-api.com/api/basho/"

# Output folder
OUTPUT_DIR = "basho_json"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_and_save_basho(basho_id):
    url = f"{BASE_URL}{basho_id}"
    print(f"Fetching {url} ...")
    resp = requests.get(url)
    resp.raise_for_status()  # raises error if request fails
    
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"{timestamp}_basho_{basho_id}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(resp.text)

    print(f"✅ Saved {filepath}")

if __name__ == "__main__":
    for bid in basho_ids:
        try:
            fetch_and_save_basho(bid)
        except Exception as e:
            print(f"❌ Failed for basho {bid}: {e}")
