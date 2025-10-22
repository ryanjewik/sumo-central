from dotenv import load_dotenv
import os, sys, json, time

load_dotenv()

# Read S3-related environment variables after dotenv is loaded
S3_BUCKET = os.getenv("S3_BUCKET")
S3_REGION = os.getenv("AWS_REGION")
S3_PREFIX = os.getenv("S3_PREFIX", "sumo-api-calls/")



def _save_to_s3(data, prefix, name):
    fname = f"{name}.json"
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"
    s3_key = f"{prefix}{fname}"
    _s3_put_json(data, s3_key)
    
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

def _safe_json(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), indent=2)