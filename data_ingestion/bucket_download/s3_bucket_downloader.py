import os
import boto3
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
S3_REGION = os.getenv("AWS_REGION")
S3_PREFIX = os.getenv("S3_PREFIX", "sumo-api-calls/")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

LOCAL_BASE = Path("downloaded_s3")  # Change as needed

s3 = boto3.client(
    "s3",
    region_name=S3_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN or None,
)

def download_all(prefixes):
    paginator = s3.get_paginator("list_objects_v2")
    for prefix in prefixes:
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]

                # Skip common "directory" objects or empty keys
                if not key or key.endswith("/"):
                    continue

                # Ignore keys that reference banzuke or torikumi datasets
                k_low = key.lower()
                if "banzuke" in k_low or "torikumi" in k_low:
                    print(f"Skipping (filtered): {key}")
                    continue

                # Build local path mirroring the S3 key under LOCAL_BASE/<prefix>/<relpath>
                rel_path = os.path.relpath(key, prefix)
                local_path = LOCAL_BASE / prefix.strip("/") / rel_path if rel_path != '.' else LOCAL_BASE / prefix.strip("/")

                # If file already exists locally, skip download
                if local_path.exists():
                    print(f"Skipping (exists): {key} -> {local_path}")
                    continue

                local_path.parent.mkdir(parents=True, exist_ok=True)
                print(f"Downloading {key} -> {local_path}")
                s3.download_file(S3_BUCKET, key, str(local_path))

if __name__ == "__main__":
    prefixes = ["sumo-api-calls/", "sumo-webhooks/"]
    download_all(prefixes)
    print("Download complete.")
