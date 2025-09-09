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

import requests
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor

# -----------------------------
# Configuration via env
# -----------------------------
USER_AGENT = os.getenv(
    "SUMO_UA",
    "SumoCentralBot/1.0 (+https://your-site.example; contact@your-email.example)"
)

# Postgres
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USERNAME = os.getenv("DB_USERNAME", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# S3
S3_BUCKET = os.getenv("S3_BUCKET") or os.getenv("AWS_S3_BUCKET") or ""
S3_PREFIX = os.getenv("S3_PREFIX") or os.getenv("AWS_S3_PREFIX") or "rikishi_headshots/"
AWS_REGION = os.getenv("AWS_REGION") or ""

# Wikimedia OAuth2 (optional—but recommended for api.wikimedia.org)
WIKIMEDIA_CLIENT_ID = os.getenv("WIKIMEDIA_CLIENT_ID", "")
WIKIMEDIA_CLIENT_SECRET = os.getenv("WIKIMEDIA_CLIENT_SECRET", "")

# Ingest behavior
PREFER_FULLRES = False         # False = use thumbnail when available
SAVE_PUBLIC = True             # put S3 object ACL public-read (adjust for your bucket policy)
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
    ext = mimetypes.guess_extension(content_type or "") or os.path.splitext(chosen_url.split("?")[0])[1] or ".jpg"
    if not ext.startswith("."):
        ext = "." + ext

    base_name = f"{safe_slug(tried[0])}-{hashlib.sha256((chosen_url or '').encode()).hexdigest()[:8]}{ext}"
    s3_key = f"{S3_PREFIX.rstrip('/')}/{base_name}".lstrip("/")
    s3_url = s3_put_bytes(blob, S3_BUCKET, s3_key, content_type=content_type or "image/jpeg", public=SAVE_PUBLIC)

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
        mime=info.get("mime"),
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

def load_shikona_list(limit: int = 0) -> list:
    q = "SELECT shikona FROM rikishi WHERE shikona IS NOT NULL"
    if limit and limit > 0:
        q += " LIMIT %s"
        params = (limit,)
    else:
        params = None
    with pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(q, params) if params else cur.execute(q)
            rows = cur.fetchall()
    return [row["shikona"] for row in rows]

# -----------------------------
# Runner
# -----------------------------
def main():
    if not S3_BUCKET:
        raise SystemExit("S3_BUCKET is required (env).")
    shikonas = load_shikona_list(BATCH_LIMIT)
    total = len(shikonas)
    print(f"Loaded {total} shikona rows")

    success = 0
    misses = 0

    for idx, s in enumerate(shikonas, 1):
        variants = list(extract_name_variants(s))
        if not variants:
            variants = [s]
        try:
            rec = fetch_headshot_to_s3_for_terms(variants)
            if rec and rec.s3_url:
                success += 1
                print(f"[{idx}/{total}] OK  {s}  → {rec.s3_url}  (license: {rec.license})")
            else:
                misses += 1
                tried = ", ".join(variants[:3]) + ("..." if len(variants) > 3 else "")
                print(f"[{idx}/{total}] MISS {s} (tried: {tried})")
        except Exception as e:
            misses += 1
            print(f"[{idx}/{total}] ERROR {s}: {e}")

        if SLEEP_BETWEEN_ROWS > 0:
            time.sleep(SLEEP_BETWEEN_ROWS)

    print(f"Done. success={success} misses={misses} total={total}")

if __name__ == "__main__":
    main()
