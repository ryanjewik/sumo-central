#!/usr/bin/env python3
import os
import re
import time
import random
import hashlib
import mimetypes
import urllib.parse as ul
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple, Iterable
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache

from dotenv import load_dotenv
import requests
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
import io
import pymongo
from PIL import Image


load_dotenv()
# -----------------------------
# Configuration via env
# -----------------------------
USER_AGENT = os.getenv(
    "SUMO_UA",
    "SumoCentralBot/1.0 (+https://your-site.example; contact@your-email.example)"
)


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

# Postgres
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USERNAME = os.getenv("DB_USERNAME", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# Mongo
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "sumo")

# S3
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX")
AWS_REGION = os.getenv("AWS_REGION") or ""

# Wikimedia OAuth2 (optional—but recommended for api.wikimedia.org)
WIKIMEDIA_CLIENT_ID = os.getenv("WIKIMEDIA_CLIENT_ID", "")
WIKIMEDIA_CLIENT_SECRET = os.getenv("WIKIMEDIA_CLIENT_SECRET", "")

# Ingest behavior
PREFER_FULLRES = False         # False = use thumbnail when available
SAVE_PUBLIC = env_bool("SAVE_PUBLIC", False)  # default False since your bucket disables ACLs
BATCH_LIMIT = int(os.getenv("BATCH_LIMIT", "0"))   # 0 = all rows; else limit for testing
SLEEP_BETWEEN_ROWS = float(os.getenv("SLEEP_BETWEEN_ROWS", "0.0"))  # extra pause per row (seconds)

# Rate limiting per host (req/second). Be conservative.
# 1.2 rps ≈ 4320 req/hr. A single rikishi may cost ~3-4 requests (search + getentities + imageinfo + download).
# Increase carefully if you are confident about your quotas and backoff handling.
PER_HOST_RPS = float(os.getenv("PER_HOST_RPS", "1.2"))

# -----------------------------
# HTTP session & token manager
# -----------------------------
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": USER_AGENT,
    "Accept-Encoding": "gzip, deflate, br",
})

# Parallelism/config
WORKER_THREADS = int(os.getenv("WORKER_THREADS", str(max(4, (os.cpu_count() or 2) * 2))))

# Thread-local storage for per-thread DB connections
_thread_local = threading.local()

# Lock to protect shared _last_request_ts updates (thread-safety)
_last_request_ts_lock = threading.Lock()

class WikimediaTokenManager:
    TOKEN_URL = "https://meta.wikimedia.org/w/rest.php/oauth2/access_token"
    def __init__(self, client_id: Optional[str], client_secret: Optional[str]):
        self.client_id = client_id
        self.client_secret = client_secret
        self._token = None
        self._expires_at = 0.0

    def _needs_refresh(self) -> bool:
        return not self._token or (time.time() >= self._expires_at - 60)

    def get_bearer(self) -> Optional[str]:
        if not (self.client_id and self.client_secret):
            return None
        if self._needs_refresh():
            r = requests.post(
                self.TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                headers={"User-Agent": USER_AGENT},
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
            self._token = data.get("access_token")
            ttl = int(data.get("expires_in", 3600))
            self._expires_at = time.time() + max(60, ttl)
        return self._token

TOKEN_MGR = WikimediaTokenManager(WIKIMEDIA_CLIENT_ID, WIKIMEDIA_CLIENT_SECRET)

def auth_headers_for(url: str) -> Dict[str, str]:
    """
    Attach Authorization ONLY for api.wikimedia.org (REST API Gateway).
    Not used for Action API (wikidata/commons) which don't require auth for reads.
    """
    host = ul.urlparse(url).netloc.lower()
    if host.endswith("api.wikimedia.org"):
        tok = TOKEN_MGR.get_bearer()
        if tok:
            return {"Authorization": f"Bearer {tok}"}
    return {}

# -----------------------------
# Host-aware polite GET with backoff and per-host RPS cap
# -----------------------------
_last_request_ts: Dict[str, float] = {}

def _respect_rps(url: str, per_host_rps: float):
    if per_host_rps <= 0:
        return
    host = ul.urlparse(url).netloc
    min_interval = 1.0 / per_host_rps
    with _last_request_ts_lock:
        last = _last_request_ts.get(host, 0.0)
        since = time.time() - last
    if since < min_interval:
        time.sleep(min_interval - since + random.uniform(0, 0.05))

def polite_get(url: str, params: Dict[str, Any] = None, max_retries: int = 7, timeout: int = 40) -> requests.Response:
    delay = 0.7
    for attempt in range(max_retries):
        _respect_rps(url, PER_HOST_RPS)
        headers = {"User-Agent": USER_AGENT}
        headers.update(auth_headers_for(url))
        r = SESSION.get(url, params=params, headers=headers, timeout=timeout)
        with _last_request_ts_lock:
            _last_request_ts[ul.urlparse(url).netloc] = time.time()
        if r.status_code in (429, 503):
            ra = r.headers.get("Retry-After")
            sleep_s = float(ra) if ra else delay * (2 ** attempt) + random.uniform(0, 0.8)
            time.sleep(min(sleep_s, 90))
            continue
        if 500 <= r.status_code < 600:
            time.sleep(delay * (2 ** attempt) + random.uniform(0, 0.8))
            continue
        r.raise_for_status()
        return r
    raise RuntimeError(f"Failed GET {url} after {max_retries} retries")

# -----------------------------
# Name parsing (original → kanji → kana)
# -----------------------------
KANJI_RE = re.compile(r"[\u4E00-\u9FFF々ヵヶ]+")
HIRAGANA_RE = re.compile(r"[\u3040-\u309Fー]+")
KATAKANA_RE = re.compile(r"[\u30A0-\u30FFー]+")

def extract_name_variants(shikona: str) -> Iterable[str]:
    """
    Input examples:
      'Asanoyama (朝乃山(あさのやま))'
      'Hoshōryū'
      '大栄翔(だいえいしょう)'
    Yield order: original (clean), kanji, hiragana, katakana (if present).
    """
    s = shikona.strip()
    # Original without trailing parentheses space
    original = re.sub(r"\s*\(.*\)\s*$", "", s).strip()
    variants = []
    if original:
        variants.append(original)

    # Find anything inside parentheses to hunt for scripts
    inside = re.findall(r"\((.*?)\)", s)
    for chunk in inside:
        kanji = KANJI_RE.findall(chunk)
        kana_h = HIRAGANA_RE.findall(chunk)
        kana_k = KATAKANA_RE.findall(chunk)
        for k in kanji:
            if k not in variants:
                variants.append(k)
        for h in kana_h:
            if h not in variants:
                variants.append(h)
        for k in kana_k:
            if k not in variants:
                variants.append(k)

    # Also scan the whole string as fallback
    for k in KANJI_RE.findall(s):
        if k not in variants:
            variants.append(k)
    for h in HIRAGANA_RE.findall(s):
        if h not in variants:
            variants.append(h)
    for k in KATAKANA_RE.findall(s):
        if k not in variants:
            variants.append(k)

    # Deduplicate while preserving order
    seen = set()
    out = []
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out

# -----------------------------
# Wikidata → Commons helpers
# -----------------------------
def wikidata_search_item(term: str, lang: str = "en") -> Optional[str]:
    url = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbsearchentities",
        "search": term,
        "language": lang,
        "type": "item",
        "format": "json",
        "limit": 5
    }
    r = polite_get(url, params)
    hits = r.json().get("search", [])
    if not hits and lang != "ja":
        # try Japanese fallback if no English hit
        return wikidata_search_item(term, "ja")
    if not hits:
        return None

    def score(hit):
        d = (hit.get("description") or "").lower()
        l = (hit.get("label") or "").lower()
        s = 0
        if "sumo" in d or "sumo" in l:
            s += 2
        if "wrestler" in d:
            s += 1
        return -s

    hits.sort(key=score)
    return hits[0]["id"]


# Cache wikidata lookups (term,lang) to avoid repeated network calls for the same terms
wikidata_search_item = lru_cache(maxsize=4096)(wikidata_search_item)

def wikidata_get_p18_filename(qid: str) -> Optional[str]:
    url = "https://www.wikidata.org/w/api.php"
    params = {"action": "wbgetentities", "ids": qid, "props": "claims", "format": "json"}
    r = polite_get(url, params)
    ent = r.json().get("entities", {}).get(qid, {})
    p18 = ent.get("claims", {}).get("P18", [])
    if not p18:
        return None
    mainsnak = p18[0].get("mainsnak", {})
    return (mainsnak.get("datavalue") or {}).get("value")


# Cache p18 filename lookups per QID
wikidata_get_p18_filename = lru_cache(maxsize=8192)(wikidata_get_p18_filename)

def commons_imageinfo(filename: str, thumb_px: int = 800) -> Optional[Dict[str, Any]]:
    url = "https://commons.wikimedia.org/w/api.php"
    params = {
        "action": "query",
        "titles": f"File:{filename}" if not filename.startswith("File:") else filename,
        "prop": "imageinfo",
        "iiprop": "url|mime|size|extmetadata",
        "iiurlwidth": str(thumb_px),
        "format": "json",
    }
    r = polite_get(url, params)
    page = next(iter(r.json().get("query", {}).get("pages", {}).values()), {})
    ii = (page.get("imageinfo") or [])
    return ii[0] if ii else None


# Cache commons imageinfo (filename,thumb_px). Thumb size unlikely to change during a run.
commons_imageinfo = lru_cache(maxsize=8192)(commons_imageinfo)

def download_bytes(url: str) -> Tuple[bytes, Optional[str]]:
    r = polite_get(url)
    return r.content, r.headers.get("Content-Type")

# -----------------------------
# S3
# -----------------------------
S3 = boto3.client("s3", region_name=AWS_REGION or None)

def s3_put_bytes(data: bytes, bucket: str, key: str, content_type: Optional[str] = None, public: bool = True) -> str:
    extra = {"ContentType": content_type} if content_type else {}
    if public:
        extra["ACL"] = "public-read"
    S3.put_object(Bucket=bucket, Key=key, Body=data, **extra)
    if AWS_REGION and AWS_REGION != "us-east-1":
        return f"https://{bucket}.s3.{AWS_REGION}.amazonaws.com/{ul.quote(key)}"
    return f"https://{bucket}.s3.amazonaws.com/{ul.quote(key)}"

def safe_slug(s: str) -> str:
    return "".join(c.lower() if c.isalnum() else "-" for c in s).strip("-")

# -----------------------------
# Data model
# -----------------------------
@dataclass
class ImageRecord:
    shikona: str
    tried_terms: list
    qid: Optional[str]
    commons_title: Optional[str]
    commons_source_url: Optional[str]
    image_url: Optional[str]
    width: Optional[int]
    height: Optional[int]
    mime: Optional[str]
    license: Optional[str]
    license_url: Optional[str]
    attribution_html: Optional[str]
    credit_html: Optional[str]
    s3_key: Optional[str]
    s3_url: Optional[str]

# -----------------------------
# Core fetch
# -----------------------------
def fetch_headshot_to_s3_for_terms(terms: Iterable[str]) -> Optional[ImageRecord]:
    tried = []
    qid = None
    filename = None
    for term in terms:
        tried.append(term)
        qid = wikidata_search_item(term)
        if not qid:
            continue
        filename = wikidata_get_p18_filename(qid)
        if filename:
            break

    if not filename:
        return ImageRecord(
            shikona=terms[0] if isinstance(terms, list) and terms else "",
            tried_terms=tried,
            qid=qid,
            commons_title=None,
            commons_source_url=None,
            image_url=None,
            width=None,
            height=None,
            mime=None,
            license=None,
            license_url=None,
            attribution_html=None,
            credit_html=None,
            s3_key=None,
            s3_url=None,
        )

    info = commons_imageinfo(filename)
    if not info:
        return None

    extmeta = info.get("extmetadata", {}) or {}
    image_url = info.get("url")
    thumb_url = info.get("thumburl")
    chosen_url = image_url if (PREFER_FULLRES or not thumb_url) else thumb_url

    blob, content_type = download_bytes(chosen_url)

    # Convert to WebP before uploading
    try:
        im = Image.open(io.BytesIO(blob))
        # Convert palette/transparent images appropriately
        has_alpha = (im.mode in ("RGBA", "LA")) or (im.mode == "P" and "transparency" in im.info)
        if has_alpha:
            out_im = im.convert("RGBA")
        else:
            out_im = im.convert("RGB")

        out_buf = io.BytesIO()
        # quality 85 is a reasonable default; method 6 for slower/better
        out_im.save(out_buf, format="WEBP", quality=85, method=6)
        webp_data = out_buf.getvalue()
        content_type = "image/webp"
        ext = ".webp"
        blob_to_upload = webp_data
    except Exception:
        # If conversion fails, fall back to original bytes
        blob_to_upload = blob
        ext = mimetypes.guess_extension(content_type or "") or os.path.splitext(chosen_url.split("?")[0])[1] or ".jpg"
        if not ext.startswith("."):
            ext = "." + ext

    base_name = f"{safe_slug(tried[0])}-{hashlib.sha256((chosen_url or '').encode()).hexdigest()[:8]}{ext}"
    s3_key = f"{(S3_PREFIX or '').rstrip('/')}/{base_name}".lstrip("/")
    s3_url = s3_put_bytes(blob_to_upload, S3_BUCKET, s3_key, content_type=content_type or "image/jpeg", public=SAVE_PUBLIC)

    commons_title = filename if filename.startswith("File:") else f"File:{filename}"
    commons_source_url = f"https://commons.wikimedia.org/wiki/{ul.quote(commons_title)}"

    return ImageRecord(
        shikona=tried[0],
        tried_terms=tried,
        qid=qid,
        commons_title=commons_title,
        commons_source_url=commons_source_url,
        image_url=chosen_url,
        width=info.get("width"),
        height=info.get("height"),
        mime=content_type,
        license=(extmeta.get("LicenseShortName") or {}).get("value"),
        license_url=(extmeta.get("LicenseUrl") or {}).get("value"),
        attribution_html=(extmeta.get("Artist") or {}).get("value"),
        credit_html=(extmeta.get("Credit") or {}).get("value"),
        s3_key=s3_key,
        s3_url=s3_url,
    )

# -----------------------------
# Postgres helpers
# -----------------------------
def pg_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        connect_timeout=10,
    )


def get_thread_conn():
    """Return a per-thread persistent psycopg2 connection (created lazily).

    This avoids creating/closing connections for every row while staying safe
    under Threads. Connections are left open until process exit, which is
    fine for short-lived batch runs. If you want explicit cleanup, call
    `close_thread_conn()` from the main thread after workers shutdown.
    """
    conn = getattr(_thread_local, "conn", None)
    if conn is None:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            connect_timeout=10,
        )
        _thread_local.conn = conn
    return conn


def close_thread_conn():
    conn = getattr(_thread_local, "conn", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
        _thread_local.conn = None

def load_shikona_list(limit: int = 0, start_from: int = 0) -> list:
    q = "SELECT id, shikona FROM rikishi WHERE shikona IS NOT NULL"
    params = []
    if start_from > 0:
        q += " OFFSET %s"
        params.append(start_from)
    if limit and limit > 0:
        q += " LIMIT %s"
        params.append(limit)
    with pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if params:
                cur.execute(q, tuple(params))
            else:
                cur.execute(q)
            rows = cur.fetchall()
    # return list of (id, shikona) tuples
    out = []
    for row in rows:
        out.append((row.get("id"), row.get("shikona")))
    return out

def update_rikishi_image(pg_id: int, rec: ImageRecord):
    """Update the Postgres rikishi row by id with the S3 URL and metadata."""
    q = """
        UPDATE rikishi
        SET
            -- store uploaded S3 URL and key
            s3_url = %s,
            s3_key = %s,
            -- original/Commons image URL
            image_url = %s,
            commons_source_url = %s,
            -- basic metadata
            width = %s,
            height = %s,
            mime = %s,
            license = %s,
            license_url = %s,
            attribution_html = %s,
            credit_html = %s
        WHERE id = %s
    """
    conn = get_thread_conn()
    with conn.cursor() as cur:
        cur.execute(q, (
            rec.s3_url,
            rec.s3_key,
            rec.image_url,
            rec.commons_source_url,
            rec.width,
            rec.height,
            rec.mime,
            rec.license,
            rec.license_url,
            rec.attribution_html,
            rec.credit_html,
            pg_id,
        ))
    conn.commit()


def update_mongo_pfp(pg_id, s3_url: str):
    """Update the rikishi_pages document in MongoDB by matching the 'id' field with pg_id and set 'pfp_url'.

    Tries both the raw pg_id and its string form if no match found.
    """
    if not MONGO_URI:
        # No Mongo configured
        return
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[MONGO_DB_NAME]
        coll = db.get_collection("rikishi_pages")
        res = coll.update_one({"id": pg_id}, {"$set": {"pfp_url": s3_url}})
        if res.matched_count == 0:
            # Try string form
            coll.update_one({"id": str(pg_id)}, {"$set": {"pfp_url": s3_url}})
        client.close()
    except Exception as e:
        print(f"Mongo update error for id {pg_id}: {e}")

# -----------------------------
# Runner
# -----------------------------
def main():
    if not S3_BUCKET:
        raise SystemExit("S3_BUCKET is required (env).")
    # Start from 9055th row (0-based offset)
    START_FROM = 0
    shikonas = load_shikona_list(BATCH_LIMIT, start_from=START_FROM)
    total = len(shikonas)
    print(f"Loaded {total} shikona rows (starting from {START_FROM})")
    # Thread-safe counters
    counters = {"success": 0, "misses": 0}
    counters_lock = threading.Lock()

    def process_row(idx_row_tuple):
        idx, row = idx_row_tuple
        pg_id, s = row
        variants = list(extract_name_variants(s))
        if not variants:
            variants = [s]
        try:
            rec = fetch_headshot_to_s3_for_terms(variants)
            if rec and rec.s3_url:
                with counters_lock:
                    counters["success"] += 1
                print(f"[{idx}/{total}] OK  {s}  → {rec.s3_url}  (license: {rec.license})")
                try:
                    update_rikishi_image(pg_id, rec)
                except Exception as e:
                    print(f"Postgres update error for id {pg_id}: {e}")

                try:
                    update_mongo_pfp(pg_id, rec.s3_url)
                except Exception as e:
                    print(f"Mongo update error for id {pg_id}: {e}")
            else:
                with counters_lock:
                    counters["misses"] += 1
                tried = ", ".join(variants[:3]) + ("..." if len(variants) > 3 else "")
                print(f"[{idx}/{total}] MISS {s} (tried: {tried})")
        except Exception as e:
            with counters_lock:
                counters["misses"] += 1
            print(f"[{idx}/{total}] ERROR {s}: {e}")

        if SLEEP_BETWEEN_ROWS > 0:
            time.sleep(SLEEP_BETWEEN_ROWS)

    # Run workers concurrently; enumerate rows starting at 1 for nicer logging
    with ThreadPoolExecutor(max_workers=WORKER_THREADS) as ex:
        futures = {ex.submit(process_row, (i + 1, r)): (i + 1, r) for i, r in enumerate(shikonas)}
        for fut in as_completed(futures):
            # propagate exceptions if needed (already printed in worker)
            try:
                fut.result()
            except Exception as e:
                idx, _ = futures[fut]
                print(f"Worker exception processing row {idx}: {e}")

    # Optionally attempt to close the thread-local connection in the main thread
    try:
        # This only closes the main thread's connection if one was created; worker threads'
        # connections will be cleaned up by process exit. For long-running services you
        # may wish to implement a stronger cleanup strategy.
        close_thread_conn()
    except Exception:
        pass

    print(f"Done. success={counters['success']} misses={counters['misses']} total={total}")

if __name__ == "__main__":
    main()
