"""Small helpers for Mongo URI normalization used by DAGs.

This module lives next to the DAGs so importing it from a DAG is simple
(`from mongo_utils import sanitize_mongo_uri`). It implements a minimal,
well-tested sanitizer that strips querystrings, optionally converts to
mongodb+srv when requested or detected, and removes explicit ports for SRV
URIs.
"""
from urllib.parse import urlparse, urlunparse
from typing import Optional, Dict


def sanitize_mongo_uri(uri: str, host: Optional[str] = None, extras: Optional[Dict] = None) -> str:
    """Return a sanitized MongoDB URI suitable for passing to PyMongo.

    Behavior:
    - Strip any querystring (remove Airflow __extra__ additions).
    - If extras.use_srv is truthy or the host looks like Atlas ('.mongodb.net')
      or the URI already contains '+srv', ensure the scheme is mongodb+srv://
      and drop any explicit port.

    The function is intentionally conservative: it does a best-effort parse
    and falls back to simple string operations if parsing fails.
    """
    if not uri:
        raise ValueError("empty mongo uri")

    # Keep only the portion before any querystring
    base = uri.split("?", 1)[0]

    extras = extras or {}
    use_srv = bool(extras.get("use_srv")) or (host and ".mongodb.net" in host) or ("+srv" in base)

    if use_srv:
        # Normalize scheme
        if base.startswith("mongodb://"):
            base = base.replace("mongodb://", "mongodb+srv://", 1)
        elif base.startswith("mongo://"):
            base = base.replace("mongo://", "mongodb+srv://", 1)

        # Try parsing and remove port if present
        try:
            parsed = urlparse(base)
            netloc = parsed.netloc
            if "@" in netloc:
                userinfo, hostpart = netloc.rsplit("@", 1)
                host_only = hostpart.split(":", 1)[0]
                new_netloc = f"{userinfo}@{host_only}"
            else:
                host_only = netloc.split(":", 1)[0]
                new_netloc = host_only
            base = urlunparse((parsed.scheme, new_netloc, parsed.path or "", "", "", ""))
        except Exception:
            # Best-effort fallback: remove :port just before first '/'
            try:
                before_path, sep, after = base.partition("/")
                if ":" in before_path:
                    before_path = before_path.split(":", 1)[0]
                base = before_path + (sep + after if after else "")
            except Exception:
                pass

    return base
