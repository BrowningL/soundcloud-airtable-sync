import os
import time
import asyncio
import threading
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

# --- NEW: Import random for jittered delay ---
import random
import json

import logging
import requests
from flask import Flask, jsonify, request, abort
from playwright.async_api import async_playwright

# Postgres
import psycopg2
from psycopg2.extras import RealDictCursor

# --- NEW IMPORTS FOR CATALOGUE HEALTH ---
import difflib
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
# --- END NEW IMPORTS ---

# --- NEW FUNCTION FOR TELEGRAM ALERTS ---
def send_telegram_alert(message: str):
    """Sends an alert using a Telegram bot if credentials are set."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not all([bot_token, chat_id]):
        streams_logger.warning("Telegram env variables not set. Skipping alert.")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message}

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status() # Raise an exception for bad status codes
        streams_logger.info(f"Successfully sent Telegram alert to chat ID {chat_id}")
    except requests.exceptions.RequestException as e:
        streams_logger.error(f"Failed to send Telegram alert: {e}")

#
# ────────────────────────────────────────────────────────────────────────────────
# LOGGING (Railway captures stdout/stderr and python logging)
#
# ────────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger("railway")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
logger.setLevel(logging.INFO)
streams_logger = logging.getLogger("streams")
followers_logger = logging.getLogger("followers")
scheduler_logger = logging.getLogger("scheduler")
# --- NEW LOGGER FOR HEALTH CHECK ---
health_logger = logging.getLogger("catalogue_health")


# Try to import the catalogue health worker with defensive logging
ch_run = None

#
# ────────────────────────────────────────────────────────────────────────────────
# CONFIG
#
# ────────────────────────────────────────────────────────────────────────────────
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY") or os.getenv("AT_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID") or os.getenv("AT_BASE_ID") or "appAmLhYAVcmKmRC3"
if not AIRTABLE_API_KEY or not AIRTABLE_API_KEY.startswith("pat"):
    raise RuntimeError("Set AIRTABLE_API_KEY to a valid Airtable Personal Access Token (starts with 'pat').")

DATABASE_URL = os.getenv("DATABASE_URL")
OUTPUT_TARGET = os.getenv("OUTPUT_TARGET", "postgres").lower()
AUTOMATION_TOKEN = os.getenv("AUTOMATION_TOKEN")
LOCAL_TZ = os.getenv("LOCAL_TZ", "Europe/London")

# --- PROXY CONFIGURATION ---
PROXY_URL = os.getenv("PROXY_URL")
proxies = {
    "http": PROXY_URL,
    "https": PROXY_URL,
} if PROXY_URL else None

if proxies:
    logger.info("Proxy configured and will be used for Spotify requests.")
    
# --- OFFICIAL SPOTIFY API ENDPOINTS ---
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_SEARCH_URL = "https://api.spotify.com/v1/search"
SPOTIFY_PLAYLIST_URL = "https://api.spotify.com/v1/playlists/"
# This is the direct, unofficial endpoint. It may be heavily protected.
SPOTIFY_PATHFINDER_URL = "https://api-partner.spotify.com/pathfinder/v2/query"


# ----- Catalogue (ISRC list) -----
CATALOGUE_TABLE = os.getenv("CATALOGUE_TABLE", "Catalogue")
CATALOGUE_VIEW = os.getenv("CATALOGUE_VIEW", "Inner Catalogue")
CATALOGUE_ISRC_FIELD = os.getenv("CATALOGUE_ISRC_FIELD", "ISRC")
CATALOGUE_ARTIST_FIELD = os.getenv("CATALOGUE_ARTIST_FIELD", "Artist")
CATALOGUE_TITLE_FIELD = os.getenv("CATALOGUE_TITLE_FIELD", "Track Title")

# ----- Track Playcounts (Airtable)
PLAYCOUNTS_TABLE = os.getenv("PLAYCOUNTS_TABLE", "Spotify Streams")
PLAYCOUNTS_LINK_FIELD = os.getenv("PLAYCOUNTS_LINK_FIELD", "ISRC")
PLAYCOUNTS_DATE_FIELD = os.getenv("PLAYCOUNTS_DATE_FIELD", "Date")
PLAYCOUNTS_COUNT_FIELD = os.getenv("PLAYCOUNTS_COUNT_FIELD", "Playcount")
PLAYCOUNTS_DELTA_FIELD = os.getenv("PLAYCOUNTS_DELTA_FIELD", "Delta")
PLAYCOUNTS_KEY_FIELD = os.getenv("PLAYCOUNTS_KEY_FIELD")

# ----- Playlists master (Airtable is source of truth) -----
PLAYLISTS_TABLE = os.getenv("PLAYLISTS_TABLE", "Playlists")
PLAYLISTS_NAME_FIELD = os.getenv("PLAYLISTS_NAME_FIELD", "Playlist Name")
PLAYLISTS_ID_FIELD = os.getenv("PLAYLISTS_ID_FIELD", "Playlist ID")
PLAYLISTS_WEB_URL_FIELD = os.getenv("PLAYLISTS_WEB_URL_FIELD", "Playlist Web URL")

# ----- Playlist Followers (Airtable) -----
FOLLOWERS_TABLE = os.getenv("FOLLOWERS_TABLE", "Playlist Followers")
FOLLOWERS_LINK_FIELD = os.getenv("FOLLOWERS_LINK_FIELD", "Playlist")
FOLLOWERS_DATE_FIELD = os.getenv("FOLLOWERS_DATE_FIELD", "Date")
FOLLOWERS_COUNT_FIELD = os.getenv("FOLLOWERS_COUNT_FIELD", "Followers")
FOLLOWERS_DELTA_FIELD = os.getenv("FOLLOWERS_DELTA_FIELD", "Delta")
FOLLOWERS_ALLOW_NEGATIVE = (os.getenv("FOLLOWERS_ALLOW_NEGATIVE", "true").lower() in ("1", "true", "yes"))

# Spotify creds
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "YOUR_SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "YOUR_SPOTIFY_CLIENT_SECRET")
SPOTIFY_REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")


# Spotify web GraphQL for streams (unchanged)
OPERATION_NAME = "getAlbum"
PERSISTED_HASH = "97dd13a1f28c80d66115a13697a7ffd94fe3bebdb94da42159456e1d82bfee76"
CAPTURED_VARS = {"locale": "", "offset": 0, "limit": 50}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

airtable_sleep = float(os.getenv("AT_SLEEP", "0.2"))
spotify_sleep = float(os.getenv("SPOTIFY_SLEEP", "0.15"))

# ── Lag config (simple hard floor + caps + scheduler) ───────────────────────────
LAG_MIN_TOTAL = int(os.getenv("LAG_MIN_TOTAL", "0"))
CAP_CHECKPOINT_RATIO = float(os.getenv("CAP_CHECKPOINT_RATIO", "0.30"))
CAP_DAILY_RATIO = float(os.getenv("CAP_DAILY_RATIO", "0.60"))
ENABLE_SCHEDULER = os.getenv("ENABLE_SCHEDULER", "true").lower() in ("1","true","yes")
SCHEDULE_EVERY_HOURS = int(os.getenv("SCHEDULE_EVERY_HOURS", "6"))

#
# ────────────────────────────────────────────────────────────────────────────────
# NETWORK HELPERS (WITH RETRY LOGIC)
#
# ────────────────────────────────────────────────────────────────────────────────
def _spotify_request_with_retries(method: str, url: str, **kwargs) -> requests.Response:
    """A wrapper for requests that includes retry logic for network/proxy errors."""
    if proxies:
        kwargs["proxies"] = proxies
    for attempt in range(3):
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request to {url} failed on attempt {attempt + 1}/3. Error: {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                logger.error(f"Request to {url} failed after 3 attempts.")
                raise
    raise requests.exceptions.RequestException(f"All retry attempts to {url} failed.")

#
# ────────────────────────────────────────────────────────────────────────────────
# Airtable helpers
#
# ────────────────────────────────────────────────────────────────────────────────
def at_headers():
    return {"Authorization": f"Bearer {AIRTABLE_API_KEY}", "Content-Type": "application/json"}

def at_url(table: str) -> str:
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{requests.utils.quote(table)}"

def _q(s: str) -> str:
    return s.replace("'", "\\'")

def at_paginate(table: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    out, offset = [], None
    while True:
        q = dict(params)
        if offset:
            q["offset"] = offset
        r = requests.get(at_url(table), headers=at_headers(), params=q, timeout=60)
        r.raise_for_status()
        data = r.json()
        out.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    return out

def at_batch_patch(table: str, records: List[Dict[str, Any]]):
    i = 0
    while i < len(records):
        chunk = records[i:i+10]
        r = requests.patch(at_url(table), headers=at_headers(), json={"records": chunk}, timeout=60)
        if r.status_code not in (200, 201):
            error_details = r.text
            try:
                error_details = r.json()
            except Exception:
                pass
            logger.error(f"Airtable batch patch error details: {error_details}")
            raise RuntimeError(f"Airtable error {r.status_code}: {r.text}")
        i += 10
        time.sleep(airtable_sleep)

def _norm_lookup(val: Any) -> Optional[str]:
    """Normalize Airtable lookup/rollup/text to a single string."""
    if val is None: return None
    if isinstance(val, list):
        vals = [str(v).strip() for v in val if v is not None and str(v).strip() != ""]
        return " & ".join(vals) if vals else None
    s = str(val).strip()
    return s or None

def catalogue_index() -> Dict[str, Dict[str, Optional[str]]]:
    """
    Return mapping:
      { ISRC_UPPER: { "air_id": <catalogue_record_id>, "artist": <str|None>, "title": <str|None> } }
    """
    params = {
        "view": CATALOGUE_VIEW,
        "pageSize": 100,
        "fields[]": [CATALOGUE_ISRC_FIELD, CATALOGUE_ARTIST_FIELD, CATALOGUE_TITLE_FIELD],
    }
    rows = at_paginate(CATALOGUE_TABLE, params)
    out: Dict[str, Dict[str, Optional[str]]] = {}
    for rec in rows:
        f = rec.get("fields", {})
        isrc = (f.get(CATALOGUE_ISRC_FIELD) or "").strip().upper()
        if not isrc:
            continue
        out[isrc] = {
            "air_id": rec["id"],
            "artist": _norm_lookup(f.get(CATALOGUE_ARTIST_FIELD)),
            "title": _norm_lookup(f.get(CATALOGUE_TITLE_FIELD)),
        }
    return out

#
# ────────────────────────────────────────────────────────────────────────────────
# Postgres helpers
#
# ────────────────────────────────────────────────────────────────────────────────
def db_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def db_ensure_platform(cur, platform: str = "spotify"):
    cur.execute("INSERT INTO platform_dim(platform) VALUES (%s) ON CONFLICT DO NOTHING", (platform,))

def db_upsert_track(cur, isrc: str, artist: Optional[str] = None, title: Optional[str] = None) -> str:
    cur.execute("""
        INSERT INTO track_dim(isrc, artist, title)
        VALUES (%s, %s, %s)
        ON CONFLICT (isrc) DO UPDATE
          SET artist = COALESCE(EXCLUDED.artist, track_dim.artist),
              title  = COALESCE(EXCLUDED.title,  track_dim.title)
        RETURNING track_uid
    """, (isrc, artist, title))
    return cur.fetchone()["track_uid"]

def db_upsert_stream(cur, platform: str, track_uid: str, day_iso: str, playcount: int):
    cur.execute("""
        INSERT INTO streams(platform, track_uid, stream_date, playcount)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (platform, track_uid, stream_date)
        DO UPDATE SET playcount = EXCLUDED.playcount
    """, (platform, track_uid, day_iso, playcount))

def db_upsert_playlist_followers(cur, platform: str, playlist_id_urn: str, day_iso: str, followers: int, playlist_name: str = None):
    cur.execute("""
        INSERT INTO playlist_followers(platform, playlist_id, snapshot_date, followers, playlist_name)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (platform, playlist_id, snapshot_date)
        DO UPDATE SET followers = EXCLUDED.followers,
                      playlist_name = COALESCE(EXCLUDED.playlist_name, playlist_followers.playlist_name)
    """, (platform, playlist_id_urn, day_iso, followers, playlist_name))

# --- START: NEW DB HELPERS FOR CATALOGUE HEALTH ---
def db_ensure_catalogue_health_schema(cur):
    """Creates the catalogue_health_status table if it doesn't exist."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS catalogue_health_status (
            check_date DATE NOT NULL,
            track_uid UUID NOT NULL REFERENCES track_dim(track_uid),
            apple_music_status BOOLEAN NOT NULL,
            spotify_status BOOLEAN NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (check_date, track_uid)
        );
    """)
    health_logger.info("Ensured catalogue_health_status table exists.")

def db_upsert_catalogue_health_status(cur, check_date_iso: str, track_uid: str, apple_status: bool, spotify_status: bool):
    """Inserts or updates a health status record for a given track and date."""
    cur.execute("""
        INSERT INTO catalogue_health_status (check_date, track_uid, apple_music_status, spotify_status)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (check_date, track_uid)
        DO UPDATE SET
            apple_music_status = EXCLUDED.apple_music_status,
            spotify_status = EXCLUDED.spotify_status,
            updated_at = NOW();
    """, (check_date_iso, track_uid, apple_status, spotify_status))
# --- END: NEW DB HELPERS FOR CATALOGUE HEALTH ---


#
# ────────────────────────────────────────────────────────────────────────────────
# Track Playcounts helpers (Airtable)
#
# ────────────────────────────────────────────────────────────────────────────────
def _key(isrc_code: str, day_iso: str) -> str:
    return f"{isrc_code}|{day_iso}"

def find_today_by_isrc(isrc_code: str, day_iso: str) -> Optional[str]:
    if PLAYCOUNTS_KEY_FIELD:
        formula = f"{{{PLAYCOUNTS_KEY_FIELD}}} = '{_q(_key(isrc_code, day_iso))}'"
    else:
        formula = (
            f"AND("
            f"SEARCH('{_q(isrc_code)}', ARRAYJOIN({{{PLAYCOUNTS_LINK_FIELD}}})),"
            f"IS_SAME({{{PLAYCOUNTS_DATE_FIELD}}}, '{day_iso}', 'day')"
            f")"
        )
    r = requests.get(
        at_url(PLAYCOUNTS_TABLE),
        headers=at_headers(),
        params={"filterByFormula": formula, "pageSize": 1},
        timeout=60,
    )
    if r.status_code != 200:
        return None
    recs = r.json().get("records", [])
    return recs[0]["id"] if recs else None

def prev_count_by_isrc(isrc_code: str, before_iso: str) -> Optional[int]:
    clauses = [
        f"SEARCH('{_q(isrc_code)}', ARRAYJOIN({{{PLAYCOUNTS_LINK_FIELD}}})),"
        f"IS_BEFORE({{{PLAYCOUNTS_DATE_FIELD}}}, '{before_iso}')",
        f"{{{PLAYCOUNTS_COUNT_FIELD}}} > 0"
    ]
    formula = "AND(" + ",".join(clauses) + ")"
    params = {
        "filterByFormula": formula,
        "pageSize": 1,
        "sort[0][field]": PLAYCOUNTS_DATE_FIELD,
        "sort[0][direction]": "desc",
    }
    r = requests.get(at_url(PLAYCOUNTS_TABLE), headers=at_headers(), params=params, timeout=60)
    if r.status_code != 200: return None
    recs = r.json().get("records", [])
    if not recs: return None
    try:
        return int(recs[0]["fields"].get(PLAYCOUNTS_COUNT_FIELD, 0) or 0)
    except Exception:
        return None

def upsert_count(linked_catalogue_rec_id: str, isrc_code: str, day_iso: str, count: int):
    existing_id = find_today_by_isrc(isrc_code, day_iso)
    prev = prev_count_by_isrc(isrc_code, day_iso)
    delta = None
    if prev is not None:
        raw = count - prev
        delta = raw if (count > 0 and prev > 0 and raw > 0) else 0

    fields = {
        PLAYCOUNTS_LINK_FIELD: [linked_catalogue_rec_id],
        PLAYCOUNTS_DATE_FIELD: day_iso,
        PLAYCOUNTS_COUNT_FIELD: count,
    }
    if PLAYCOUNTS_KEY_FIELD:
        fields[PLAYCOUNTS_KEY_FIELD] = _key(isrc_code, day_iso)
    if delta is not None:
        fields[PLAYCOUNTS_DELTA_FIELD] = delta

    payload = {"records": [{"fields": fields}]}
    if existing_id:
        payload["records"][0]["id"] = existing_id
        r = requests.patch(at_url(PLAYCOUNTS_TABLE), headers=at_headers(), json=payload, timeout=60)
    else:
        r = requests.post(at_url(PLAYCOUNTS_TABLE), headers=at_headers(), json=payload, timeout=60)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Airtable error {r.status_code}: {r.text}")
    time.sleep(airtable_sleep)

#
# ────────────────────────────────────────────────────────────────────────────────
# Playlists master (Airtable list → which playlists to track)
#
# ────────────────────────────────────────────────────────────────────────────────
def playlists_index_from_airtable():
    rows = at_paginate(PLAYLISTS_TABLE, {
        "pageSize": 100,
        "fields[]": [PLAYLISTS_NAME_FIELD, PLAYLISTS_ID_FIELD, PLAYLISTS_WEB_URL_FIELD]
    })
    out = {}
    for r in rows:
        f = r.get("fields", {})
        raw = f.get(PLAYLISTS_ID_FIELD) or f.get(PLAYLISTS_WEB_URL_FIELD)
        urn = to_spotify_playlist_urn(raw)
        name = f.get(PLAYLISTS_NAME_FIELD)
        if urn:
            out[r["id"]] = {"playlist_id_urn": urn, "name": name}
    return out

#
# ────────────────────────────────────────────────────────────────────────────────
# Spotify helpers for streams
#
# ────────────────────────────────────────────────────────────────────────────────
def parse_spotify_playlist_id(val: Optional[str]) -> Optional[str]:
    if not val: return None
    s = str(val).strip()
    if s.startswith("spotify:playlist:"):
        return s.split(":")[-1]
    try:
        u = urlparse(s)
        if "spotify" in (u.netloc or "") and u.path:
            parts = u.path.strip("/").split("/")
            if len(parts) >= 2 and parts[0] == "playlist":
                return parts[1]
    except Exception:
        pass
    return s

def to_spotify_playlist_urn(raw: Optional[str]) -> Optional[str]:
    pid = parse_spotify_playlist_id(raw)
    return f"spotify:playlist:{pid}" if pid else None

def urn_to_plain_id(urn: str) -> str:
    return urn.split(":")[-1] if urn and ":" in urn else urn

def _check_token():
    if not AUTOMATION_TOKEN:
        return
    token = request.headers.get("x-automation-token") or request.args.get("token")
    if token != AUTOMATION_TOKEN:
        abort(403)

def _has_spotify_creds():
    return bool(CLIENT_ID and CLIENT_SECRET and CLIENT_ID != "YOUR_SPOTIFY_CLIENT_ID" and CLIENT_SECRET != "YOUR_SPOTIFY_CLIENT_SECRET")

def get_spotify_token_refreshed() -> str:
    """Gets a new Spotify access token using the refresh token THROUGH the proxy."""
    if not SPOTIFY_REFRESH_TOKEN:
        raise ValueError("SPOTIFY_REFRESH_TOKEN is not set.")
    
    logger.info("Requesting new Spotify access token using refresh token (through proxy)...")
    
    r = _spotify_request_with_retries(
        "post",
        SPOTIFY_TOKEN_URL,
        auth=(CLIENT_ID, CLIENT_SECRET),
        data={
            "grant_type": "refresh_token",
            "refresh_token": SPOTIFY_REFRESH_TOKEN,
        },
        timeout=30
    )
    token = r.json().get("access_token")
    if not token:
        raise RuntimeError("Failed to get access_token from Spotify.")
    logger.info("Successfully refreshed Spotify access token.")
    return token

def search_track(isrc: str, bearer: str) -> Optional[Tuple[str, str, str, Optional[str]]]:
    """
    Return (track_id, album_id, track_name, artists_joined) for the given ISRC,
    or None if not found.
    """
    r = _spotify_request_with_retries("get", SPOTIFY_SEARCH_URL,
                                      headers={"Authorization": f"Bearer {bearer}"},
                                      params={"q": f"isrc:{isrc}", "type": "track", "limit": 5},
                                      timeout=60)
    items = r.json().get("tracks", {}).get("items", [])
    if not items:
        return None

    best = None
    for t in items:
        if t.get("external_ids", {}).get("isrc", "").upper() == isrc.upper():
            best = t
            break
    if best is None:
        best = items[0]

    track_id = best.get("id")
    album_id = best.get("album", {}).get("id")
    track_name = best.get("name")
    artists = [a.get("name") for a in (best.get("artists") or []) if a.get("name")]
    artists_joined = " & ".join(artists) if artists else None

    if not (track_id and album_id):
        return None
    return track_id, album_id, track_name, artists_joined

async def sniff_tokens() -> Tuple[str, Optional[str]]:
    proxy_server = None
    if PROXY_URL:
        parsed_url = urlparse(PROXY_URL)
        proxy_server = {
            "server": f"{parsed_url.scheme}://{parsed_url.hostname}:{parsed_url.port}",
            "username": parsed_url.username,
            "password": parsed_url.password
        }

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"], proxy=proxy_server)
        ctx = await browser.new_context(user_agent=USER_AGENT)
        page = await ctx.new_page()
        fut = asyncio.get_event_loop().create_future()
        def on_resp(resp):
            if "/pathfinder/v2/query" in resp.url and resp.status == 200:
                hdrs = resp.request.headers
                auth = hdrs.get("authorization")
                if auth and auth.startswith("Bearer "):
                    tok = auth.split(" ", 1)[1]
                    cli = hdrs.get("client-token")
                    if not fut.done():
                        fut.set_result((tok, cli))
        page.on("response", on_resp)
        # We still need to visit the web player to get the tokens, even if we hit the API directly later.
        await page.goto("https://open.spotify.com/")
        try:
            return await asyncio.wait_for(fut, timeout=30)
        finally:
            await browser.close()

def fetch_album(album_id: str, web_token: str, client_token: Optional[str]) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {web_token}", "User-Agent": USER_AGENT, "content-type": "application/json"}
    if client_token:
        headers["Client-Token"] = client_token
    body = {
        "operationName": OPERATION_NAME,
        "variables": {**CAPTURED_VARS, "uri": f"spotify:album:{album_id}"},
        "extensions": {"persistedQuery": {"version": 1, "sha256Hash": PERSISTED_HASH}},
    }
    try:
        # Using the retry wrapper and the direct pathfinder URL
        r = _spotify_request_with_retries("post", SPOTIFY_PATHFINDER_URL, headers=headers, json=body, timeout=30)
        response_json = r.json()
        if not response_json.get("data"):
            streams_logger.warning(f"Received empty 'data' from Spotify for album {album_id}. Likely a proxy/IP block.")
            return {}
        return response_json
    except requests.exceptions.RequestException:
        # The error is already logged by the wrapper, so we just return empty
        return {}


#
# ────────────────────────────────────────────────────────────────────────────────
# Generic Airtable delta recompute
#
# ────────────────────────────────────────────────────────────────────────────────
def backfill_table_deltas(table: str, link_field: str, date_field: str, count_field: str, delta_field: str, clamp_negative: bool) -> int:
    recs = at_paginate(table, {"pageSize": 100, "sort[0][field]": date_field, "sort[0][direction]": "asc"})
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for rec in recs:
        links = rec.get("fields", {}).get(link_field, [])
        if not links:
            continue
        key = links[0]
        groups.setdefault(key, []).append(rec)

    updates: List[Dict[str, Any]] = []
    changed = 0
    for _, items in groups.items():
        items.sort(key=lambda r: r.get("fields", {}).get(date_field, "") or "")
        prev_val: Optional[int] = None
        for rec in items:
            f = rec.get("fields", {})
            try:
                cur = int(f.get(count_field, 0) or 0)
            except Exception:
                cur = 0

            if prev_val is None:
                new_delta = None
            else:
                if cur > 0 and prev_val > 0:
                    raw = cur - prev_val
                    new_delta = (raw if raw >= 0 else (0 if clamp_negative else raw))
                else:
                    new_delta = 0

            if new_delta is not None and f.get(delta_field) != new_delta:
                updates.append({"id": rec["id"], "fields": {delta_field: new_delta}})
                changed += 1

            prev_val = cur

            if len(updates) == 10:
                at_batch_patch(table, updates)
                updates.clear()
                time.sleep(airtable_sleep)

    if updates:
        at_batch_patch(table, updates)
    return changed

def backfill_deltas_for_all_tracks() -> int:
    return backfill_table_deltas(PLAYCOUNTS_TABLE, PLAYCOUNTS_LINK_FIELD, PLAYCOUNTS_DATE_FIELD, PLAYCOUNTS_COUNT_FIELD, PLAYCOUNTS_DELTA_FIELD, clamp_negative=True)

def backfill_deltas_for_followers() -> int:
    return backfill_table_deltas(FOLLOWERS_TABLE, FOLLOWERS_LINK_FIELD, FOLLOWERS_DATE_FIELD, FOLLOWERS_COUNT_FIELD, FOLLOWERS_DELTA_FIELD, clamp_negative=not FOLLOWERS_ALLOW_NEGATIVE)

#
# ────────────────────────────────────────────────────────────────────────────────
# Lag schema & catalogue totals (DB-only; idempotent)
# ... (this section is unchanged)
#
# ────────────────────────────────────────────────────────────────────────────────
def db_ensure_lag_schema(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_totals (
            day date PRIMARY KEY,
            total_delta bigint NOT NULL DEFAULT 0,
            finalized boolean NOT NULL DEFAULT false,
            updated_at timestamptz NOT NULL DEFAULT now()
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lag_credits (
            day date PRIMARY KEY,
            moved_today bigint NOT NULL DEFAULT 0,
            moved_alltime bigint NOT NULL DEFAULT 0,
            updated_at timestamptz NOT NULL DEFAULT now()
        );
    """)

def db_upsert_daily_total(cur, day_iso: str, total_delta: int, finalized: bool):
    cur.execute("""
        INSERT INTO daily_totals(day,total_delta,finalized)
        VALUES (%s,%s,%s)
        ON CONFLICT (day) DO UPDATE
        SET total_delta=EXCLUDED.total_delta,
            finalized = CASE WHEN daily_totals.finalized THEN daily_totals.finalized ELSE EXCLUDED.finalized END,
            updated_at=now();
    """, (day_iso, total_delta, finalized))

def db_catalogue_delta_for_day(cur, day_iso: str) -> int:
    cur.execute("""
        WITH today AS (
          SELECT s.track_uid, s.playcount AS pc_day
          FROM streams s
          WHERE s.platform='spotify' AND s.stream_date=%s
        ),
        prev AS (
          SELECT s1.track_uid,
                 (SELECT s2.playcount
                   FROM streams s2
                  WHERE s2.platform='spotify'
                    AND s2.track_uid = s1.track_uid
                    AND s2.stream_date < %s
                  ORDER BY s2.stream_date DESC
                  LIMIT 1) AS pc_prev
            FROM streams s1
           WHERE s1.platform='spotify'
           GROUP BY s1.track_uid
        )
        SELECT COALESCE(SUM(GREATEST(0, t.pc_day - COALESCE(p.pc_prev,0))),0) AS delta_sum
        FROM today t LEFT JOIN prev p USING (track_uid);
    """, (day_iso, day_iso))
    return int((cur.fetchone() or {}).get("delta_sum", 0))

def db_get_lag_queue(cur, today_iso: str) -> List[str]:
    cur.execute("""
        SELECT day FROM daily_totals
        WHERE day < %s AND finalized=false AND total_delta < %s
        ORDER BY day ASC
    """, (today_iso, LAG_MIN_TOTAL))
    return [row["day"].isoformat() for row in cur.fetchall()]

def db_mark_finalized_if_ready(cur, day_iso: str):
    cur.execute("SELECT total_delta FROM daily_totals WHERE day=%s", (day_iso,))
    row = cur.fetchone()
    if row and int(row["total_delta"]) >= LAG_MIN_TOTAL:
        cur.execute("UPDATE daily_totals SET finalized=true, updated_at=now() WHERE day=%s", (day_iso,))

def _cap_amount_for_anchor(cur, anchor_day: str, remaining_to_move: int) -> int:
    target = LAG_MIN_TOTAL
    cap_checkpoint = int(target * CAP_CHECKPOINT_RATIO)
    cap_daily      = int(target * CAP_DAILY_RATIO)
    cur.execute("SELECT moved_today FROM lag_credits WHERE day=%s", (anchor_day,))
    row = cur.fetchone() or {"moved_today": 0}
    room_today = max(0, cap_daily - int(row["moved_today"] or 0))
    return max(0, min(remaining_to_move, cap_checkpoint, room_today))

def _bump_lag_credits(cur, anchor_day: str, moved: int):
    if moved <= 0: return
    cur.execute("""
        INSERT INTO lag_credits(day, moved_today, moved_alltime)
        VALUES (%s, %s, %s)
        ON CONFLICT (day) DO UPDATE
        SET moved_today = lag_credits.moved_today + EXCLUDED.moved_today,
            moved_alltime = lag_credits.moved_alltime + EXCLUDED.moved_alltime,
            updated_at = now();
    """, (anchor_day, moved, moved))

def db_today_increments(cur, today_iso: str) -> List[Tuple[str,int,int]]:
    cur.execute("""
        WITH today AS (
          SELECT s.track_uid, s.playcount AS pc_today
          FROM streams s
          WHERE s.platform='spotify' AND s.stream_date=%s
        ),
        prev AS (
          SELECT s1.track_uid,
                 (SELECT s2.playcount
                   FROM streams s2
                  WHERE s2.platform='spotify'
                    AND s2.track_uid = s1.track_uid
                    AND s2.stream_date < %s
                  ORDER BY s2.stream_date DESC
                  LIMIT 1) AS pc_prev
            FROM streams s1
           WHERE s1.platform='spotify'
           GROUP BY s1.track_uid
        )
        SELECT t.track_uid,
               COALESCE(p.pc_prev, 0) AS prev_playcount,
               GREATEST(0, t.pc_today - COALESCE(p.pc_prev, 0)) AS increment
        FROM today t
        LEFT JOIN prev p USING (track_uid)
        WHERE GREATEST(0, t.pc_today - COALESCE(p.pc_prev, 0)) > 0;
    """, (today_iso, today_iso))
    return [(r["track_uid"], r["prev_playcount"], r["increment"]) for r in cur.fetchall()]

def db_apply_lag_transfer(cur, from_day: str, to_day: str, track_uid: str, from_prev_pc: int, amount: int):
    if amount <= 0: return
    cur.execute("""
        UPDATE streams
        SET playcount = playcount - %s
        WHERE platform='spotify' AND track_uid=%s AND stream_date=%s
    """, (amount, track_uid, from_day))
    to_day_new_pc = from_prev_pc + amount
    cur.execute("""
        INSERT INTO streams(platform, track_uid, stream_date, playcount)
        VALUES ('spotify', %s, %s, %s)
        ON CONFLICT (platform, track_uid, stream_date)
        DO UPDATE SET playcount = streams.playcount + EXCLUDED.playcount
    """, (track_uid, to_day, to_day_new_pc))

#
# ────────────────────────────────────────────────────────────────────────────────
# MAIN WORKER: Track Streams
#
# ────────────────────────────────────────────────────────────────────────────────
async def run_once(day_override: Optional[str] = None, attempt_idx: int = 1, output_target: str = OUTPUT_TARGET) -> Dict[str, Any]:
    # FIX: The script now runs for the most recently completed day (yesterday).
    day_iso = day_override or (date.today() - timedelta(days=1)).isoformat()
    
    catalogue = catalogue_index()
    cat_isrcs = list(catalogue.keys())
    
    is_debug_run = False # Set to False to run all tracks
    if is_debug_run:
        streams_logger.info("--- RUNNING IN DEBUG MODE: PROCESSING FIRST 10 TRACKS ONLY ---")
        cat_isrcs = cat_isrcs[:10]
    
    streams_logger.info("starting run: tracks=%d output=%s attempt=%d date=%s", len(cat_isrcs), output_target, attempt_idx, day_iso)

    try:
        search_token = get_search_token()
        web_token, client_token = await sniff_tokens()
    except Exception as e:
        streams_logger.error(f"Failed to get auth tokens, cannot proceed. Error: {e}")
        error_message = f"ALERT: Spotify stream run failed during token setup. Error: {e}"
        send_telegram_alert(error_message)
        return {"error": "Token acquisition failed", "details": str(e)}


    processed, errors, sum_pc, sum_delta_like = 0, 0, 0, 0
    records_to_process = []
    
    for i, isrc in enumerate(cat_isrcs):
        if (i+1) % 50 == 0:
            streams_logger.info("progress: %d/%d", i+1, len(cat_isrcs))
        
        playcount, delta_like = None, None
        api_artist, api_title = None, None
        cat = catalogue.get(isrc, {})
        cat_artist = cat.get("artist")
        cat_title = cat.get("title")

        try:
            track_info = search_track(isrc, search_token)
            if not track_info:
                time.sleep(spotify_sleep)
                continue
            
            track_id, album_id, api_title, api_artist = track_info
            album_data = fetch_album(album_id, web_token, client_token)
            
            tracks = (album_data.get("data", {}).get("albumUnion", {}).get("tracksV2", {}).get("items", []))
            for item in tracks:
                t = item.get("track")
                if t and t.get("uri") == f"spotify:track:{track_id}":
                    raw = t.get("playcount")
                    if raw and str(raw).isdigit():
                        playcount = int(raw)
                        if output_target in ("airtable", "both"):
                            prev = prev_count_by_isrc(isrc, day_iso)
                            if prev is not None and playcount > prev:
                                delta_like = playcount - prev
                        break
            
            records_to_process.append({
                "isrc": isrc,
                "artist": cat_artist or api_artist,
                "title": cat_title or api_title,
                "air_id": cat.get("air_id"),
                "playcount": playcount,
            })
            
            processed += 1
            if playcount is not None: sum_pc += playcount
            if delta_like is not None: sum_delta_like += delta_like
            time.sleep(spotify_sleep)
        
        except requests.exceptions.RequestException as e:
            # Catch network/proxy errors specifically and log as a warning
            streams_logger.warning(f"Skipping ISRC {isrc} due to network/proxy error: {e}")
            errors += 1
            continue # Continue to the next ISRC
        except Exception as e:
            streams_logger.error("error processing ISRC=%s: %s", isrc, e, exc_info=True)
            errors += 1

    if output_target in ("airtable", "both"):
        streams_logger.info("writing %d records to Airtable", len(records_to_process))
        for record in records_to_process:
            if record["air_id"] and record["playcount"] is not None:
                try:
                    upsert_count(record["air_id"], record["isrc"], day_iso, record["playcount"])
                except Exception as e:
                    streams_logger.error("[airtable] error upserting ISRC=%s: %s", record["isrc"], e)
                    errors += 1

    daily_delta_pg = 0
    if output_target in ("postgres", "both"):
        streams_logger.info("writing %d records to Postgres", len(records_to_process))
        conn = None
        try:
            conn = db_conn()
            with conn.cursor() as cur:
                db_ensure_platform(cur, platform="spotify")
                for record in records_to_process:
                    if record["playcount"] is not None:
                        try:
                            track_uid = db_upsert_track(cur, record["isrc"], record["artist"], record["title"])
                            db_upsert_stream(cur, "spotify", track_uid, day_iso, record["playcount"])
                        except Exception as e:
                            streams_logger.error("[postgres] error processing ISRC=%s in batch: %s", record["isrc"], e)
                            errors += 1
            conn.commit()
            
            streams_logger.info("[postgres] calculating total daily streams for %s", day_iso)
            with conn.cursor() as cur:
                daily_delta_pg = db_catalogue_delta_for_day(cur, day_iso)
            
        except psycopg2.Error as e:
            streams_logger.exception("[postgres] stream write transaction failed: %s", e)
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    if output_target in ("postgres", "both") and LAG_MIN_TOTAL > 0:
        conn = None
        try:
            conn = db_conn()
            with conn.cursor() as cur:
                db_ensure_lag_schema(cur)
                total_delta = db_catalogue_delta_for_day(cur, day_iso)
                db_upsert_daily_total(cur, day_iso, total_delta, finalized=(total_delta >= LAG_MIN_TOTAL))

                lag_q = db_get_lag_queue(cur, day_iso)
                if not lag_q:
                    streams_logger.info("[lag] no past days require backfilling")
                else:
                    streams_logger.info("[lag] queue: %s", lag_q)
                    increments = db_today_increments(cur, day_iso)
                    increments.sort(key=lambda r: r[2], reverse=True)
                    
                    for to_day in lag_q:
                        cur.execute("SELECT total_delta FROM daily_totals WHERE day=%s", (to_day,))
                        to_day_current = int(cur.fetchone()["total_delta"])
                        needed = LAG_MIN_TOTAL - to_day_current
                        if needed <= 0: continue

                        capped_needed = _cap_amount_for_anchor(cur, day_iso, needed)
                        if capped_needed <= 0: break

                        moved_this_day = 0
                        for i in range(len(increments)):
                            track_uid, prev_pc, inc = increments[i]
                            if inc <= 0: continue
                            
                            can_move = min(inc, capped_needed - moved_this_day)
                            if can_move <= 0: continue

                            db_apply_lag_transfer(cur, day_iso, to_day, track_uid, prev_pc, can_move)
                            increments[i] = (track_uid, prev_pc, inc - can_move)
                            moved_this_day += can_move
                            if moved_this_day >= capped_needed: break
                        
                        _bump_lag_credits(cur, day_iso, moved_this_day)

                all_affected_days = set(lag_q + [day_iso])
                for d in all_affected_days:
                    recalc_delta = db_catalogue_delta_for_day(cur, d)
                    db_upsert_daily_total(cur, d, recalc_delta, finalized=False)
                    db_mark_finalized_if_ready(cur, d)
            
            conn.commit()
        except psycopg2.Error as e:
            streams_logger.exception("[postgres] lag processing transaction failed: %s", e)
            if conn: conn.rollback()
        finally:
            if conn: conn.close()
    
    final_delta = daily_delta_pg if output_target in ("postgres", "both") else sum_delta_like
    stats = {
        "processed": processed,
        "errors": errors,
        "date": day_iso,
        "output": output_target,
        "total_lifetime_streams_found": sum_pc,
        "total_daily_streams_gained": final_delta,
        "attempt": attempt_idx,
    }

    log_summary = (
        f"Run completed for {day_iso}. "
        f"Processed: {processed}, Errors: {errors}. "
        f"Total lifetime streams found: {sum_pc:,}. "
        f"Total daily streams gained: {final_delta:,}."
    )
    streams_logger.info(log_summary)
    
    # Send the alert if the run completed but found no streams
    if processed > 0 and sum_pc == 0:
        error_message = f"ALERT: Spotify stream run for {day_iso} completed but found 0 total streams. Please check the system."
        send_telegram_alert(error_message)

    return stats

#
# ────────────────────────────────────────────────────────────────────────────────
# MAIN WORKER: Playlist Followers
#
# ────────────────────────────────────────────────────────────────────────────────
def run_playlist_followers(day_override: Optional[str] = None):
    day_iso = day_override or date.today().isoformat()

    playlists = playlists_index_from_airtable()
    followers_logger.info("starting followers run: playlists=%d date=%s", len(playlists), day_iso)

    if not _has_spotify_creds():
        raise RuntimeError("Spotify API credentials not set for playlist followers.")
    
    bearer = get_search_token()
    processed, errors = 0, 0
    records_to_write_at = []
    records_to_write_pg = []

    for airtable_rec_id, p_info in playlists.items():
        urn = p_info["playlist_id_urn"]
        name = p_info["name"]
        plain_id = urn_to_plain_id(urn)
        try:
            url = f"{SPOTIFY_PLAYLIST_URL}{plain_id}?fields=followers(total)"
            r = _spotify_request_with_retries("get", url,
                                             headers={"Authorization": f"Bearer {bearer}"}, timeout=30)
            
            followers = int(r.json().get("followers", {}).get("total", 0) or 0)
            
            records_to_write_at.append({
                "fields": {
                    FOLLOWERS_LINK_FIELD: [airtable_rec_id],
                    FOLLOWERS_DATE_FIELD: day_iso,
                    FOLLOWERS_COUNT_FIELD: followers,
                }
            })
            records_to_write_pg.append({
                "urn": urn,
                "name": name,
                "followers": followers
            })
            processed += 1
            time.sleep(airtable_sleep)
        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 404:
                followers_logger.warning("playlist not found (404): id=%s name=%s", plain_id, name)
                continue
            followers_logger.error("error processing playlist id=%s name=%s: %s", plain_id, name, e)
            errors += 1
        except requests.exceptions.RequestException as e:
            followers_logger.error("error processing playlist id=%s name=%s: %s", plain_id, name, e)
            errors += 1
    
    if OUTPUT_TARGET in ("airtable", "both") and records_to_write_at:
        try:
            followers_logger.info("writing %d follower counts to Airtable", len(records_to_write_at))
            i = 0
            while i < len(records_to_write_at):
                chunk = records_to_write_at[i:i+10]
                r = requests.post(at_url(FOLLOWERS_TABLE), headers=at_headers(), json={"records": chunk}, timeout=60)
                if r.status_code not in (200, 201):
                    raise RuntimeError(f"Airtable create error {r.status_code}: {r.text}")
                i += 10
                time.sleep(airtable_sleep)
        except Exception as e:
            followers_logger.error("Airtable batch update failed: %s", e)
            errors += len(records_to_write_at)

    if OUTPUT_TARGET in ("postgres", "both") and records_to_write_pg:
        followers_logger.info("writing %d follower counts to Postgres", len(records_to_write_pg))
        conn = None
        try:
            conn = db_conn()
            with conn.cursor() as cur:
                db_ensure_platform(cur, "spotify")
                for rec in records_to_write_pg:
                    db_upsert_playlist_followers(cur, "spotify", rec["urn"], day_iso, rec["followers"], rec["name"])
            conn.commit()
        except psycopg2.Error as e:
            followers_logger.exception("Postgres followers write failed: %s", e)
            if conn: conn.rollback()
            errors += len(records_to_write_pg)
        finally:
            if conn: conn.close()

    stats = {"processed": processed, "errors": errors, "date": day_iso}
    followers_logger.info("completed followers run: %s", " ".join(f"{k}={v}" for k, v in stats.items()))
    return stats


#
# ────────────────────────────────────────────────────────────────────────────────
# PLAYLIST SYNC WORKER
#
# ────────────────────────────────────────────────────────────────────────────────
sync_logger = logging.getLogger("playlist_sync")

# --- Environment variables for Playlist Sync ---
# SPOTIFY_REFRESH_TOKEN is defined in the main config section

# Table/Field names from Airtable Script
CAT_F_URI = os.getenv("CAT_F_URI", "Spotify URI")
CAT_F_ISRC = os.getenv("CAT_F_ISRC", "ISRC")

PLY_F_LAST_SNAPSHOT = os.getenv("PLY_F_LAST_SNAPSHOT", "Snapshot ID")
PLY_F_ORDER_HASH = os.getenv("PLY_F_ORDER_HASH", "Last Order Hash")
PLY_F_LAST_SYNC = os.getenv("PLY_F_LAST_SYNC", "Last Synced")
PLY_POSSIBLE_PLAYLIST_FIELDS = [
    "Playlist","Spotify Playlist","Spotify Playlist URI",
    "Spotify URL","Spotify URI","URL","URI","Playlist ID","Playlist Web URL"
]

PLACEMENTS_TABLE = os.getenv("PLACEMENTS_TABLE", "Placements")
PL_F_PLAYLIST = os.getenv("PLACEMENTS_PLAYLIST_LINK_FIELD", "Playlist")
PL_F_TRACK_LINK = os.getenv("PLACEMENTS_TRACK_LINK_FIELD", "Track Title")
PL_F_POSITION = os.getenv("PLACEMENTS_POSITION_FIELD", "Position")


def get_spotify_token_refreshed() -> str:
    """Gets a new Spotify access token using the refresh token."""
    # This is now the primary auth method for the entire script
    if not SPOTIFY_REFRESH_TOKEN:
        raise ValueError("SPOTIFY_REFRESH_TOKEN is not set.")
    
    sync_logger.info("Requesting new Spotify access token...")
    r = _spotify_request_with_retries(
        "post",
        SPOTIFY_TOKEN_URL,
        auth=(CLIENT_ID, CLIENT_SECRET),
        data={
            "grant_type": "refresh_token",
            "refresh_token": SPOTIFY_REFRESH_TOKEN,
        },
        timeout=30
    )
    token = r.json().get("access_token")
    if not token:
        raise RuntimeError("Failed to get access_token from Spotify.")
    sync_logger.info("Successfully refreshed Spotify access token.")
    return token


def extract_playlist_id(raw: Any) -> Optional[str]:
    """Extracts a Spotify playlist ID from various string formats."""
    if not raw:
        return None
    
    if isinstance(raw, list) and raw:
        val = raw[0].get("name") or raw[0].get("url") or raw[0].get("text") or raw[0]
        return extract_playlist_id(val)

    s = str(raw).strip()
    import re
    patterns = [
        r"spotify:playlist:([A-Za-z0-9]+)",
        r"playlist\/([A-Za-z0-9]+)",
        r"^([A-Za-z0-9]{22})$",
        r"([A-Za-z0-9]{22})",
    ]
    for pattern in patterns:
        match = re.search(pattern, s)
        if match:
            return match.group(1)
    return None


def get_playlist_snapshot(token: str, playlist_id: str) -> Dict[str, Any]:
    """Fetches just the snapshot_id and total track count for a playlist."""
    url = f"{SPOTIFY_PLAYLIST_URL}{playlist_id}?fields=snapshot_id,tracks(total)"
    r = _spotify_request_with_retries("get", url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    return r.json()


def fetch_all_playlist_tracks(token: str, playlist_id: str) -> List[Dict[str, Any]]:
    """Paginates through a playlist to get all its tracks."""
    items = []
    url = f"{SPOTIFY_PLAYLIST_URL}{playlist_id}/tracks?limit=100&fields=items(track(uri,name,artists(name),album(id))),next"
    
    page = 1
    while url:
        sync_logger.info(f"Fetching page {page} for playlist {playlist_id}...")
        r = _spotify_request_with_retries("get", url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
        data = r.json()
        
        for item in data.get("items", []):
            track = item.get("track")
            if not track: continue
            items.append({
                "uri": track.get("uri"),
                "name": track.get("name"),
                "artist": ", ".join([a["name"] for a in track.get("artists", []) if a.get("name")]),
                "albumId": track.get("album", {}).get("id"),
            })
        
        url = data.get("next")
        page += 1
        time.sleep(spotify_sleep)
        
    return items

def diff_placements_multi(existing: List[Dict], desired: List[Dict]) -> Dict[str, List]:
    """
    Compares existing placements with desired placements to find what needs
    to be created, updated, or deleted. Handles duplicate tracks.
    `existing`: [{"recId": str, "catId": str, "pos": int}]
    `desired`: [{"catId": str, "pos": int}]
    """
    ex_by_cat = {}
    for e in existing:
        ex_by_cat.setdefault(e["catId"], []).append({"recId": e["recId"], "pos": e["pos"]})
    for arr in ex_by_cat.values():
        arr.sort(key=lambda x: x["pos"])

    de_by_cat = {}
    for d in desired:
        de_by_cat.setdefault(d["catId"], []).append(d["pos"])
    for arr in de_by_cat.values():
        arr.sort()

    to_create, to_update, to_delete = [], [], []
    all_cat_ids = set(ex_by_cat.keys()) | set(de_by_cat.keys())

    for cat_id in all_cat_ids:
        ex = ex_by_cat.get(cat_id, [])
        de = de_by_cat.get(cat_id, [])
        match_count = min(len(ex), len(de))

        for i in range(match_count):
            if ex[i]["pos"] != de[i]:
                to_update.append({"recId": ex[i]["recId"], "pos": de[i]})
        
        for i in range(match_count, len(de)):
            to_create.append({"catId": cat_id, "pos": de[i]})
        
        for i in range(match_count, len(ex)):
            to_delete.append(ex[i]["recId"])

    return {"toCreate": to_create, "toUpdate": to_update, "toDelete": to_delete}

def at_batch_delete(table: str, record_ids: List[str]):
    """Deletes records from Airtable in batches of 10."""
    i = 0
    while i < len(record_ids):
        chunk = record_ids[i:i+10]
        params = {"records[]": chunk}
        r = requests.delete(at_url(table), headers=at_headers(), params=params, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"Airtable delete error {r.status_code}: {r.text}")
        sync_logger.info(f"Deleted {len(chunk)} records from {table}")
        i += 10
        time.sleep(airtable_sleep)

def run_playlist_sync():
    """Main worker function to sync Spotify playlists to Airtable."""
    sync_logger.info("Starting playlist sync process...")
    
    # 1. Get Spotify Token
    try:
        token = get_spotify_token_refreshed()
    except Exception as e:
        sync_logger.exception("Fatal: Could not get Spotify token.")
        return {"status": "error", "message": "Spotify authentication failed"}

    # 2. Fetch all necessary data from Airtable first
    sync_logger.info("Fetching initial data from Airtable...")
    try:
        all_playlists = at_paginate(PLAYLISTS_TABLE, {"pageSize": 100})
        all_placements_raw = at_paginate(PLACEMENTS_TABLE, {"pageSize": 100, "fields[]": [PL_F_PLAYLIST, PL_F_TRACK_LINK, PL_F_POSITION]})
        catalogue_raw = at_paginate(CATALOGUE_TABLE, {"pageSize": 100, "fields[]": [CAT_F_URI, CAT_F_ISRC]})
    except Exception as e:
        sync_logger.exception("Fatal: Could not fetch initial data from Airtable.")
        return {"status": "error", "message": "Airtable data fetch failed"}

    # 3. Build in-memory indexes
    cat_by_uri = {rec["fields"].get(CAT_F_URI): rec["id"] for rec in catalogue_raw if rec["fields"].get(CAT_F_URI)}
    
    placements_by_playlist_id = {}
    for plac in all_placements_raw:
        playlist_links = plac["fields"].get(PL_F_PLAYLIST, [])
        track_links = plac["fields"].get(PL_F_TRACK_LINK, [])
        if not playlist_links or not track_links:
            continue
        
        playlist_id = playlist_links[0]
        placements_by_playlist_id.setdefault(playlist_id, []).append({
            "recId": plac["id"],
            "catId": track_links[0],
            "pos": plac["fields"].get(PL_F_POSITION, 0)
        })
    
    sync_logger.info(f"Indexed {len(cat_by_uri)} catalogue items and placements for {len(placements_by_playlist_id)} playlists.")

    # 4. Process each playlist
    summary = []
    processed_count = 0
    for p_rec in all_playlists:
        p_name = p_rec.get("fields", {}).get(PLAYLISTS_NAME_FIELD, p_rec["id"])
        
        # Find a usable Spotify ID from any relevant field
        raw_id_val = None
        for field_name in PLY_POSSIBLE_PLAYLIST_FIELDS:
            raw_id_val = p_rec.get("fields", {}).get(field_name)
            if raw_id_val:
                break
        
        pid = extract_playlist_id(raw_id_val or p_name)
        
        if not pid:
            sync_logger.warning(f'Skipping playlist "{p_name}": No Spotify ID found.')
            summary.append({"playlist": p_name, "status": "no_spotify_id"})
            continue

        sync_logger.info(f'Processing playlist "{p_name}" (ID: {pid})...')
        
        try:
            # Snapshot Gate: Check if the playlist has changed
            meta = get_playlist_snapshot(token, pid)
            snapshot_id = meta.get("snapshot_id")
            last_snapshot = p_rec.get("fields", {}).get(PLY_F_LAST_SNAPSHOT)

            if last_snapshot and snapshot_id and last_snapshot == snapshot_id:
                sync_logger.info(f'Playlist "{p_name}" is unchanged (snapshot match).')
                summary.append({"playlist": p_name, "status": "unchanged_snapshot"})
                at_batch_patch(PLAYLISTS_TABLE, [{"id": p_rec["id"], "fields": {PLY_F_LAST_SYNC: date.today().isoformat()}}])
                continue

            # Fetch all tracks from Spotify for the changed playlist
            live_tracks = fetch_all_playlist_tracks(token, pid)
            sync_logger.info(f'Fetched {len(live_tracks)} tracks from Spotify for "{p_name}".')

            # Build the desired state of placements
            desired = []
            for i, track in enumerate(live_tracks):
                cat_id = cat_by_uri.get(track["uri"])
                if cat_id:
                    desired.append({"catId": cat_id, "pos": i + 1})
                else:
                    sync_logger.warning(f'Track URI {track["uri"]} ({track["name"]}) not in Catalogue. Skipping placement.')

            # Get existing placements and diff
            existing = placements_by_playlist_id.get(p_rec["id"], [])
            diff = diff_placements_multi(existing, desired)
            
            to_create = diff["toCreate"]
            to_update = diff["toUpdate"]
            to_delete = diff["toDelete"]
            
            sync_logger.info(f'Diff for "{p_name}": +{len(to_create)} create, ~{len(to_update)} update, -{len(to_delete)} delete.')

            # Apply changes to Airtable
            if to_delete:
                at_batch_delete(PLACEMENTS_TABLE, to_delete)
            
            if to_update:
                update_payload = [{"id": u["recId"], "fields": {PL_F_POSITION: u["pos"]}} for u in to_update]
                at_batch_patch(PLACEMENTS_TABLE, update_payload)

            if to_create:
                create_payload = [
                    {"fields": {
                        PL_F_PLAYLIST: [p_rec["id"]],
                        PL_F_TRACK_LINK: [c["catId"]],
                        PL_F_POSITION: c["pos"],
                    }} for c in to_create
                ]
                i = 0
                while i < len(create_payload):
                    chunk = create_payload[i:i+10]
                    r = requests.post(at_url(PLACEMENTS_TABLE), headers=at_headers(), json={"records": chunk}, timeout=60)
                    try:
                        r.raise_for_status()
                    except requests.exceptions.HTTPError as e:
                        sync_logger.error(f"Airtable create error on chunk: {r.status_code} - {r.text}")
                        raise e
                    i += 10
                    time.sleep(airtable_sleep)
                sync_logger.info(f"Created {len(create_payload)} new placement records.")


            # Update the playlist's metadata (snapshot, sync time)
            playlist_update_payload = {
                "id": p_rec["id"],
                "fields": {
                    PLY_F_LAST_SNAPSHOT: snapshot_id,
                    PLY_F_LAST_SYNC: date.today().isoformat()
                }
            }
            at_batch_patch(PLAYLISTS_TABLE, [playlist_update_payload])
            
            summary.append({
                "playlist": p_name, 
                "status": "synced",
                "tracks": len(live_tracks),
                "created": len(to_create),
                "updated": len(to_update),
                "deleted": len(to_delete)
            })
            processed_count += 1

        except Exception as e:
            sync_logger.exception(f'Error processing playlist "{p_name}": {e}')
            summary.append({"playlist": p_name, "status": "error", "message": str(e)})

    final_report = {
        "status": "complete",
        "playlists_processed": processed_count,
        "total_playlists": len(all_playlists),
        "summary": summary
    }
    sync_logger.info(f"Playlist sync finished. Processed {processed_count}/{len(all_playlists)} playlists.")
    return final_report


# ─── START: CATALOGUE HEALTH WORKER ───────────────────────────────────────────
def similar(a: str, b: str) -> float:
    """Return a similarity ratio between two strings (case-insensitive)."""
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio()

def check_apple_music_api(artist: str, title: str) -> bool:
    """
    Searches for a track on Apple Music with retries for rate limiting.
    """
    base_url = "https://itunes.apple.com/search"
    params = {'term': f"{artist} {title}", 'entity': 'musicTrack,album', 'country': 'GB', 'limit': 20}
    
    for attempt in range(3): # Try up to 3 times
        try:
            health_logger.info(f"[Apple] Checking for '{title}' by {artist} (Attempt {attempt+1})")
            # NOTE: Apple Music requests are NOT proxied by default, only Spotify requests.
            response = requests.get(base_url, params=params, timeout=15, headers={"User-Agent": USER_AGENT})
            
            if response.status_code == 403:
                # If forbidden, wait and retry. This is rate limiting.
                wait_time = (attempt + 1) * 15 + random.uniform(0, 5) # 15s, 30s, 45s + jitter
                health_logger.warning(f"[Apple] Received 403 (Forbidden). Rate limited. Waiting for {wait_time:.2f}s before retry.")
                time.sleep(wait_time)
                continue # Go to the next attempt
            
            response.raise_for_status() # Raise error for other codes (4xx, 5xx)
            data = response.json()
            
            for result in data.get('results', []):
                result_track = result.get('trackName', '')
                result_album = result.get('collectionName', '')
                result_artist = result.get('artistName', '')
                if similar(result_artist, artist) >= 0.85 and \
                   (similar(result_track, title) >= 0.85 or similar(result_album, title) >= 0.85):
                    health_logger.info(f"✅ [Apple] Found a match: '{result_track or result_album}'")
                    return True # Success
            
            health_logger.warning(f"❌ [Apple] No strong match found for '{title}'.")
            return False # No match found, no need to retry

        except requests.exceptions.RequestException as e:
            health_logger.error(f"❗️ [Apple] Network error for '{title}": {e}")
            if attempt < 2:
                time.sleep(5) # Wait 5s on other network errors before retry
            else:
                return False # Failed after all retries
    
    health_logger.error(f"❗️ [Apple] Failed to check '{title}' after multiple retries due to persistent 403 errors.")
    return False # Failed all attempts


def check_spotify_api(sp_client, artist: str, title: str) -> bool:
    """Searches for a track on Spotify using the Spotipy library."""
    query = f"track:{title} artist:{artist}"
    health_logger.info(f"[Spotify] Checking for '{title}' by {artist}")
    try:
        results = sp_client.search(q=query, type='track', limit=10)
        for item in results.get('tracks', {}).get('items', []):
            item_name = item.get('name', '')
            item_artists = [a.get('name', '') for a in item.get('artists', [])]
            if similar(item_name, title) >= 0.85 and \
               any(similar(a_name, artist) >= 0.85 for a_name in item_artists):
                health_logger.info(f"✅ [Spotify] Found a match: '{item_name}'")
                return True
        health_logger.warning(f"❌ [Spotify] No strong match found for '{title}'.")
        return False
    except Exception as e:
        health_logger.error(f"❗️ [Spotify] API error for '{title}': {e}")
        return False

def run_catalogue_health_worker():
    """
    Main worker to check catalogue health and store results in Postgres.
    """
    health_logger.info("--- Starting Catalogue Health Check ---")

    if not _has_spotify_creds():
        raise RuntimeError("Spotify API credentials (SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET) must be set for health checks.")

    # 1. Set up Spotify client with proxy
    try:
        token = get_spotify_token_refreshed()
        spotify_client = spotipy.Spotify(auth=token, proxies=proxies)
        health_logger.info("Spotipy client initialized successfully.")
    except Exception as e:
        health_logger.exception("Failed to initialize Spotipy client.")
        raise e

    # 2. Fetch catalogue from Airtable
    health_logger.info("Fetching catalogue from Airtable...")
    catalogue = catalogue_index()
    health_logger.info(f"Found {len(catalogue)} tracks in the catalogue.")

    # 3. Connect to DB and run checks
    conn = None
    check_date_iso = date.today().isoformat()
    checked_count = 0
    errors = 0

    try:
        conn = db_conn()
        with conn.cursor() as cur:
            # Ensure the table exists
            db_ensure_catalogue_health_schema(cur)
            conn.commit()

            # Iterate over each track from Airtable
            for isrc, track_data in catalogue.items():
                title = track_data.get('title')
                artist = track_data.get('artist')

                if not title or not artist:
                    health_logger.warning(f"Skipping ISRC {isrc} due to missing title or artist.")
                    continue
                try:
                    # Perform checks
                    apple_exists = check_apple_music_api(artist, title)
                    spotify_exists = check_spotify_api(spotify_client, artist, title)
                    
                    # --- NEW: Jittered delay to appear more human ---
                    time.sleep(random.uniform(1.0, 2.5))

                    # Get track_uid from our DB, creating the track if it's new
                    track_uid = db_upsert_track(cur, isrc, artist, title)

                    # Save the result to the database
                    db_upsert_catalogue_health_status(cur, check_date_iso, track_uid, apple_exists, spotify_exists)
                    checked_count += 1
                    if checked_count % 10 == 0:
                        conn.commit() # Commit periodically
                        health_logger.info(f"Progress: {checked_count}/{len(catalogue)} tracks checked. Committing batch.")

                except Exception as e:
                    health_logger.exception(f"An error occurred while processing ISRC {isrc}")
                    errors += 1
                    conn.rollback() # Rollback this single track's transaction

        conn.commit() # Final commit
    except psycopg2.Error as e:
        health_logger.exception("Database error during health check process: %s", e)
        if conn: conn.rollback()
        raise e
    finally:
        if conn: conn.close()
    health_logger.info(f"--- Catalogue Health Check Finished. Checked: {checked_count}, Errors: {errors} ---")

# Assign the worker function to the ch_run variable
ch_run = run_catalogue_health_worker
# ─── END: CATALOGUE HEALTH WORKER ─────────────────────────────────────────────

#
# ────────────────────────────────────────────────────────────────────────────────
# Scheduler
#
# ────────────────────────────────────────────────────────────────────────────────
_RUNNING = threading.Event()
_run_task: Optional[asyncio.Task] = None

def _schedule_loop():
    global _run_task
    delay_secs = SCHEDULE_EVERY_HOURS * 60 * 60
    scheduler_logger.info("scheduler enabled: running every %d hours", SCHEDULE_EVERY_HOURS)
    
    async def sync():
        global _run_task
        if _run_task and not _run_task.done():
            scheduler_logger.warning("skipping scheduled run, previous task still active")
            return
        
        async def run_with_retries():
            for i in range(3):
                try:
                    stats = await run_once(attempt_idx=i+1)
                    return stats
                except Exception as e:
                    scheduler_logger.exception("run_once failed (attempt %d/3): %s", i + 1, e)
                    if i < 2:
                        await asyncio.sleep(60 * (i + 1)) # 1, 2 min waits
            return {"error": "failed after 3 attempts"}
        
        _run_task = asyncio.create_task(run_with_retries())
        await _run_task

    # Main loop
    next_run_time = time.time()
    while not _RUNNING.is_set():
        if time.time() >= next_run_time:
            scheduler_logger.info("tick → running sync()")
            try:
                asyncio.run(sync())
            except Exception as e:
                scheduler_logger.error("loop error: %s", e, exc_info=True)
            next_run_time = time.time() + delay_secs
        
        time.sleep(30) # check every 30s
    scheduler_logger.info("scheduler loop exiting")


#
# ────────────────────────────────────────────────────────────────────────────────
# FLASK APP
#
# ────────────────────────────────────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/")
def index():
    return jsonify({
        "status": "ok",
        "scheduler_enabled": ENABLE_SCHEDULER,
        "output_target": OUTPUT_TARGET,
        "database_connected": bool(DATABASE_URL)
    })

@app.route("/run", methods=["POST"])
def run_once_handler():
    _check_token()
    is_async = request.args.get("async", "false").lower() == "true"
    day = request.json.get("date") if request.is_json else None
    
    async def sync():
        global _run_task
        if _run_task and not _run_task.done():
            return jsonify({"status": "error", "message": "a task is already running"}), 429
        
        _run_task = asyncio.create_task(run_once(day_override=day))
        await _run_task
        return jsonify(_run_task.result())

    if is_async:
        streams_logger.info("async start requested")
        asyncio.run(sync())
        return jsonify({"status": "started"}), 202
    else:
        return asyncio.run(sync())

@app.route("/run_followers_today", methods=["POST"])
def run_playlist_followers_handler():
    _check_token()
    day = request.json.get("date") if request.is_json else None
    try:
        stats = run_playlist_followers(day_override=day)
        return jsonify(stats)
    except Exception as e:
        followers_logger.exception("followers run failed: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/sync_playlists", methods=["POST"])
def run_playlist_sync_handler():
    # As requested, the token check has been removed.
    # WARNING: This endpoint is now public.
    # _check_token() 
    try:
        stats = run_playlist_sync()
        return jsonify(stats)
    except Exception as e:
        sync_logger.exception("Playlist sync run failed: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/backfill/<table>", methods=["POST"])
def backfill_handler(table: str):
    _check_token()
    if table == "tracks":
        changed = backfill_deltas_for_all_tracks()
    elif table == "followers":
        changed = backfill_deltas_for_followers()
    else:
        return jsonify({"error": "invalid table"}), 404
    return jsonify({"table": table, "updated_records": changed})

@app.route("/catalogue_health", methods=["POST"])
def run_ch_handler():
    # _check_token() # As requested, token check is disabled for easier triggering.
    if not ch_run:
        return jsonify({"error": "catalogue_health module not available"}), 501
    try:
        # Run the health check in a background thread to avoid long request timeouts
        threading.Thread(target=ch_run, daemon=True).start()
        return jsonify({"status": "Catalogue health check started in the background."}), 202
    except Exception as e:
        logger.exception("catalogue_health run failed: %s", e)
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    if ENABLE_SCHEDULER:
        threading.Thread(target=_schedule_loop, daemon=True).start()
    
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
