import logging
import os
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from pathlib import Path
from urllib3 import Retry
import requests

load_dotenv()

base_url = "https://sumo-api.com/api"

_session = requests.Session()
_retry = Retry(
    total=3,
    connect=3,
    read=3,
    status=3,
    backoff_factor=0.5,
    status_forcelist=(500, 502, 503, 504),
    allowed_methods=frozenset(["GET"]),
)
_adapter = HTTPAdapter(max_retries=_retry)
_session.mount("http://", _adapter)
_session.mount("https://", _adapter)
DEFAULT_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))

def get_json(path):
    """GET {base_url}{path} with timeout & retries; return dict via safe_json."""
    try:
        res = _session.get(base_url + path, timeout=DEFAULT_TIMEOUT)
        return safe_json(res)
    except Exception as e:
        logging.warning("HTTP error GET %s: %s", path, e)
        return {}
      
def safe_json(res):
    """
    Always return a dict. If API returns a list/null/HTML/error, normalize to {} or {'records': list}.
    """
    try:
        # If response isn't OK, still try to capture body but normalize to {}
        if not getattr(res, "ok", False):
            logging.warning("HTTP %s for %s", getattr(res, "status_code", "?"), getattr(res, "url", "unknown"))
        data = res.json()
        if isinstance(data, dict):
            return data
        if data is None:
            return {}
        if isinstance(data, list):
            return {"records": data}
        return {}
    except Exception as e:
        logging.warning("Error decoding JSON for %s: %s", getattr(res, "url", "unknown"), e)
        return {}