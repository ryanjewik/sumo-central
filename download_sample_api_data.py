import os
import requests
import json

SAMPLE_DIR = "sample api data"
os.makedirs(SAMPLE_DIR, exist_ok=True)

urls = [
	("rikishis.json", "https://sumo-api.com/api/rikishis"),
	("rikishi_2.json", "https://sumo-api.com/api/rikishi/2"),
	("rikishi_2_stats.json", "https://sumo-api.com/api/rikishi/2/stats"),
	("rikishi_2_matches_64.json", "https://sumo-api.com/api/rikishi/2/matches/64"),
	("rikishi_2_matches.json", "https://sumo-api.com/api/rikishi/2/matches"),
	("basho_202507.json", "https://sumo-api.com/api/basho/202507"),
	("basho_202507_banzuke_Juryo.json", "https://sumo-api.com/api/basho/202507/banzuke/Juryo"),
	("basho_202507_torikumi_Makuuchi_1.json", "https://sumo-api.com/api/basho/202507/torikumi/Makuuchi/1"),
	("kimarite_yorikiri.json", "https://sumo-api.com/api/kimarite/yorikiri"),
	("measurements_rikishi2_basho202507.json", "https://sumo-api.com/api/measurements?rikishiId=2&bashoId=202507"),
	("ranks_rikishi5.json", "https://sumo-api.com/api/ranks?rikishiId=5"),
	("shikonas_rikishi5.json", "https://sumo-api.com/api/shikonas?rikishiId=5"),
]

for filename, url in urls:
	print(f"Fetching {url}")
	try:
		resp = requests.get(url)
		resp.raise_for_status()
		data = resp.json()
		out_path = os.path.join(SAMPLE_DIR, filename)
		with open(out_path, "w", encoding="utf-8") as f:
			json.dump(data, f, ensure_ascii=False, indent=2)
		print(f"Saved {filename}")
	except Exception as e:
		print(f"Failed to fetch {url}: {e}")
