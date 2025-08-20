import os
import sys
import time
import uuid
import json
import math
import logging
import threading
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlencode, quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import psycopg2
from psycopg2.extras import execute_values

# ──────────────────────────────────────────────────────────────────────────────
# Configuration via ENV (sane defaults where possible)
# ──────────────────────────────────────────────────────────────────────────────
SPOTIFY_CLIENT_ID     = os.getenv("SPOTIFY_CLIENT_ID", "d21b640c2aff407aa394d63df79c31af")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "5171819cef7042968dd58994603eca2a")

AIRTABLE_API_KEY   = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID   = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE     = os.getenv("AIRTABLE_CATALOGUE_TABLE", "Catalogue")
AIRTABLE_VIEW      = os.getenv("AIRTABLE_VIEW")  # optional
AIRTABLE_FIELDS    = json.loads(os.getenv("AIRTABLE_FIELDS_JSON", "[]"))  # optional fields[] whitelist

# Field name fallbacks (adjust here if your schema uses different names)
FIELD_ISRC          = os.getenv("FIELD_ISRC", "ISRC")
FIELD_UPC           = os.getenv("FIELD_UPC", "UPC")
FIELD_TRACK_TITLE   = os.getenv("FIELD_TRACK_TITLE", "Track Title")
FIELD_ARTIST        = os.getenv("FIELD_ARTIST", "Primary Artist")  # fallback to 'Artist' if missing

# Apple country storefront for search/lookup
APPLE_COUNTRY       = os.getenv("APPLE_COUNTRY", "GB")

# Timescale / Postgres
DATABASE_URL        = os.getenv("DATABASE_URL")
OUTPUT_TARGET       = os.getenv("OUTPUT_TARGET", "postgres")  # keep for parity; we write to postgres

# Runtime knobs
MAX_WORKERS         = int(os.getenv("MAX_WORKERS", "12"))
HTTP_TIMEOUT        = float(os.getenv("HTTP_TIMEOUT", "15"))
RETRY_ATTEMPTS      = int(os.getenv("RETRY_ATTEMPTS", "3"))
RETRY_BACKOFF_SEC   = float(os.getenv("RETRY_BACKOFF_SEC", "1.5"))
DRY_RUN             = os.getenv("DRY_RUN", "0") == "1"
LIMIT               = int(os.getenv("LIMIT", "0"))  # 0 = no limit

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("catalogue-health")

# ──────────────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────────────
SESSION = requests.Session()

def http_request(method: str, url: str, **kwargs) -> requests.Response:
    """Robust HTTP with simple retries and Spotify 429 handling."""
    kwargs.setdefault("timeout", HTTP_TIMEOUT)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = SESSION.request(method, url, **kwargs)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "2"))
                time.sleep(min(10, retry_after))
                continue
            if 200 <= resp.status_code < 300:
                return resp
            # Retry on 5xx
            if 500 <= resp.status_code < 600:
                time.sleep(RETRY_BACKOFF_SEC * attempt)
                continue
            return resp
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                raise
            time.sleep(RETRY_BACKOFF_SEC * attempt)
    raise RuntimeError("Unreachable")

def get_field(fields: Dict[str, Any], *names: str) -> Optional[Any]:
    for n in names:
        if n in fields and fields[n] not in (None, ""):
            return fields[n]
    return None

# ──────────────────────────────────────────────────────────────────────────────
# Airtable
# ──────────────────────────────────────────────────────────────────────────────
def fetch_airtable_catalogue() -> List[Dict[str, Any]]:
    """Fetch all records from the Catalogue (optionally a specific view)."""
    assert AIRTABLE_API_KEY and AIRTABLE_BASE_ID, "Missing Airtable credentials."
    base = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{quote_plus(AIRTABLE_TABLE)}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    params = {"pageSize": 100}
    if AIRTABLE_VIEW:
        params["view"] = AIRTABLE_VIEW
    for f in AIRTABLE_FIELDS:
        params.setdefault("fields[]", [])
        params["fields[]"].append(f)

    records: List[Dict[str, Any]] = []
    offset = None
    while True:
        p = dict(params)
        if offset:
            p["offset"] = offset
        r = http_request("GET", base, headers=headers, params=p)
        data = r.json()
        chunk = data.get("records", [])
        records.extend(chunk)
        offset = data.get("offset")
        if not offset:
            break
    if LIMIT and LIMIT > 0:
        records = records[:LIMIT]
    return records

# ──────────────────────────────────────────────────────────────────────────────
# Spotify
# ──────────────────────────────────────────────────────────────────────────────
_spotify_token_lock = threading.Lock()
_spotify_token: Optional[Tuple[str, float]] = None  # (token, expiry_epoch)

def get_spotify_token() -> str:
    global _spotify_token
    with _spotify_token_lock:
        now = time.time()
        if _spotify_token and _spotify_token[1] - 30 > now:
            return _spotify_token[0]
        auth = (SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)
        data = {"grant_type": "client_credentials"}
        r = http_request("POST", "https://accounts.spotify.com/api/token", data=data, auth=auth)
        j = r.json()
        token = j["access_token"]
        expires_in = j.get("expires_in", 3600)
        _spotify_token = (token, now + expires_in)
        return token

def spotify_headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {get_spotify_token()}"}

def spotify_search_track_by_isrc(isrc: str) -> Optional[Dict[str, Any]]:
    q = f"isrc:{isrc}"
    r = http_request("GET", "https://api.spotify.com/v1/search",
                     headers=spotify_headers(),
                     params={"q": q, "type": "track", "limit": 1})
    items = r.json().get("tracks", {}).get("items", [])
    return items[0] if items else None

def spotify_search_album_by_upc(upc: str) -> Optional[Dict[str, Any]]:
    q = f"upc:{upc}"
    r = http_request("GET", "https://api.spotify.com/v1/search",
                     headers=spotify_headers(),
                     params={"q": q, "type": "album", "limit": 1})
    items = r.json().get("albums", {}).get("items", [])
    return items[0] if items else None

def spotify_search_track_fallback(title: str, artist: str) -> Optional[Dict[str, Any]]:
    q = f"track:{title} artist:{artist}"
    r = http_request("GET", "https://api.spotify.com/v1/search",
                     headers=spotify_headers(),
                     params={"q": q, "type": "track", "limit": 1})
    items = r.json().get("tracks", {}).get("items", [])
    return items[0] if items else None

# ──────────────────────────────────────────────────────────────────────────────
# Apple (iTunes Search/Lookup)
# ──────────────────────────────────────────────────────────────────────────────
def apple_lookup_isrc(isrc: str) -> Optional[Dict[str, Any]]:
    r = http_request(
        "GET",
        "https://itunes.apple.com/lookup",
        params={"isrc": isrc, "country": APPLE_COUNTRY, "entity": "song"}
    )
    res = r.json()
    results = res.get("results", [])
    return results[0] if results else None

def apple_lookup_upc(upc: str) -> Optional[Dict[str, Any]]:
    r = http_request(
        "GET",
        "https://itunes.apple.com/lookup",
        params={"upc": upc, "country": APPLE_COUNTRY}
    )
    res = r.json()
    results = res.get("results", [])
    return results[0] if results else None

def apple_search_track_fallback(title: str, artist: str) -> Optional[Dict[str, Any]]:
    term = f"{title} {artist}".strip()
    r = http_request(
        "GET",
        "https://itunes.apple.com/search",
        params={"term": term, "country": APPLE_COUNTRY, "media": "music", "entity": "song", "limit": 1}
    )
    res = r.json()
    results = res.get("results", [])
    return results[0] if results else None

def apple_best_url(hit: Dict[str, Any]) -> Optional[str]:
    # Prefer a music.apple.com link if present; fall back to track/collectionViewUrl
    for key in ("appleMusicUrl", "url", "trackViewUrl", "collectionViewUrl"):
        url = hit.get(key)
        if isinstance(url, str) and url:
            return url
    return None

# ──────────────────────────────────────────────────────────────────────────────
# TimescaleDB bootstrap
# ──────────────────────────────────────────────────────────────────────────────
DDL_CATALOGUE_HEALTH = """
CREATE TABLE IF NOT EXISTS catalogue_health (
    run_id UUID NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    airtable_id TEXT NOT NULL,
    isrc TEXT NULL,
    upc TEXT NULL,
    track_title TEXT NULL,
    artist_name TEXT NULL,
    spotify_exists BOOLEAN NOT NULL,
    apple_exists BOOLEAN NOT NULL,
    spotify_url TEXT NULL,
    apple_url TEXT NULL,
    notes TEXT NULL
);
"""

DDL_CATALOGUE_HEALTH_RUNS = """
CREATE TABLE IF NOT EXISTS catalogue_health_runs (
    run_id UUID PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    total INTEGER NOT NULL,
    both INTEGER NOT NULL,
    only_spotify INTEGER NOT NULL,
    only_apple INTEGER NOT NULL,
    not_found INTEGER NOT NULL,
    skipped INTEGER NOT NULL
);
"""

def db_connect():
    assert DATABASE_URL, "DATABASE_URL is required for TimescaleDB."
    return psycopg2.connect(DATABASE_URL)

def bootstrap_db():
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
            cur.execute(DDL_CATALOGUE_HEALTH)
            cur.execute("SELECT create_hypertable('catalogue_health','checked_at', if_not_exists => TRUE);")
            cur.execute(DDL_CATALOGUE_HEALTH_RUNS)
            conn.commit()

# ──────────────────────────────────────────────────────────────────────────────
# Core check
# ──────────────────────────────────────────────────────────────────────────────
def check_one(record: Dict[str, Any]) -> Dict[str, Any]:
    rid = record.get("id")
    fields = record.get("fields", {})

    isrc = (get_field(fields, FIELD_ISRC) or "").strip()
    upc  = (get_field(fields, FIELD_UPC) or "").strip()

    title = (get_field(fields, FIELD_TRACK_TITLE) or "").strip()
    artist = (get_field(fields, FIELD_ARTIST, "Artist", "Artists") or "").strip()

    spotify_hit = None
    apple_hit = None

    notes = []

    # Prefer identifier-based checks
    if isrc:
        spotify_hit = spotify_search_track_by_isrc(isrc)
        apple_hit   = apple_lookup_isrc(isrc)
    elif upc:
        spotify_hit = spotify_search_album_by_upc(upc)
        apple_hit   = apple_lookup_upc(upc)
    else:
        # Fallback to text search if we have enough context
        if title and artist:
            spotify_hit = spotify_search_track_fallback(title, artist)
            apple_hit   = apple_search_track_fallback(title, artist)
        else:
            notes.append("no_isrc_or_upc_or_text")
            return {
                "airtable_id": rid, "isrc": None, "upc": None,
                "track_title": title or None, "artist_name": artist or None,
                "spotify_exists": False, "apple_exists": False,
                "spotify_url": None, "apple_url": None,
                "notes": ";".join(notes) if notes else None
            }

    spotify_exists = bool(spotify_hit)
    apple_exists   = bool(apple_hit)

    spotify_url = None
    if spotify_hit:
        spotify_url = spotify_hit.get("external_urls", {}).get("spotify")
        if not spotify_url and spotify_hit.get("href"):
            spotify_url = spotify_hit["href"]

    apple_url = apple_best_url(apple_hit) if apple_hit else None

    return {
        "airtable_id": rid,
        "isrc": isrc or None,
        "upc": upc or None,
        "track_title": title or None,
        "artist_name": artist or None,
        "spotify_exists": spotify_exists,
        "apple_exists": apple_exists,
        "spotify_url": spotify_url,
        "apple_url": apple_url,
        "notes": ";".join(notes) if notes else None
    }

def persist_rows(run_id: uuid.UUID, rows: List[Dict[str, Any]]):
    if DRY_RUN:
        log.info("[DRY_RUN] Skipping DB insert for %d rows", len(rows))
        return
    if not rows:
        return
    with db_connect() as conn, conn.cursor() as cur:
        tuples = [
            (
                str(run_id),
                # checked_at uses default NOW() in DB; but send explicit for transparency
                datetime.now(timezone.utc),
                r["airtable_id"], r["isrc"], r["upc"], r["track_title"], r["artist_name"],
                r["spotify_exists"], r["apple_exists"], r["spotify_url"], r["apple_url"], r["notes"]
            )
            for r in rows
        ]
        execute_values(
            cur,
            """
            INSERT INTO catalogue_health
            (run_id, checked_at, airtable_id, isrc, upc, track_title, artist_name,
             spotify_exists, apple_exists, spotify_url, apple_url, notes)
            VALUES %s
            """,
            tuples,
            page_size=1000
        )
        conn.commit()

def persist_run_summary(run_id: uuid.UUID, started: datetime, finished: datetime, rows: List[Dict[str, Any]]):
    if DRY_RUN:
        log.info("[DRY_RUN] Skipping run summary insert")
        return
    total = len(rows)
    both = sum(1 for r in rows if r["spotify_exists"] and r["apple_exists"])
    only_spotify = sum(1 for r in rows if r["spotify_exists"] and not r["apple_exists"])
    only_apple = sum(1 for r in rows if r["apple_exists"] and not r["spotify_exists"])
    not_found = sum(1 for r in rows if not r["apple_exists"] and not r["spotify_exists"] and (r.get("notes") != "no_isrc_or_upc_or_text"))
    skipped = sum(1 for r in rows if (r.get("notes") == "no_isrc_or_upc_or_text"))

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO catalogue_health_runs
            (run_id, started_at, finished_at, total, both, only_spotify, only_apple, not_found, skipped)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (str(run_id), started, finished, total, both, only_spotify, only_apple, not_found, skipped)
        )
        conn.commit()

def main():
    start_ts = datetime.now(timezone.utc)
    run_id = uuid.uuid4()
    log.info("▶️ Catalogue Health run started | run_id=%s", run_id)

    # Bootstrap DB schema
    bootstrap_db()

    # Pull Airtable
    records = fetch_airtable_catalogue()
    log.info("Fetched %d Airtable records%s",
             len(records), f" (LIMIT={LIMIT})" if LIMIT else "")

    rows: List[Dict[str, Any]] = []

    # Concurrency to speed up remote lookups
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(check_one, rec): rec["id"] for rec in records}
        for fut in as_completed(futures):
            rid = futures[fut]
            try:
                res = fut.result()
                rows.append(res)
            except Exception as e:
                log.error("record %s failed: %s", rid, e)

    # Persist
    persist_rows(run_id, rows)

    # Summary
    both = sum(1 for r in rows if r["spotify_exists"] and r["apple_exists"])
    only_spotify = sum(1 for r in rows if r["spotify_exists"] and not r["apple_exists"])
    only_apple = sum(1 for r in rows if r["apple_exists"] and not r["spotify_exists"])
    not_found = sum(1 for r in rows if not r["apple_exists"] and not r["spotify_exists"] and (r.get("notes") != "no_isrc_or_upc_or_text"))
    skipped = sum(1 for r in rows if (r.get("notes") == "no_isrc_or_upc_or_text"))

    end_ts = datetime.now(timezone.utc)
    log.info("✅ Catalogue Health run complete | total=%d | both=%d | only_spotify=%d | only_apple=%d | not_found=%d | skipped=%d",
             len(rows), both, only_spotify, only_apple, not_found, skipped)

    # Persist run summary
    persist_run_summary(run_id, start_ts, end_ts, rows)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception("Catalogue Health run crashed: %s", e)
        sys.exit(1)
