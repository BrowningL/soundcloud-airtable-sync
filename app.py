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
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
# SPOTIFY_REFRESH_TOKEN is no longer needed


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

def at_paginate(table: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    out, offset = [], None
    while True:
        q = dict(params)
        if offset: q["offset"] = offset
        r = requests.get(at_url(table), headers=at_headers(), params=q, timeout=60)
        r.raise_for_status()
        data = r.json()
        out.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset: break
    return out

def catalogue_index() -> Dict[str, Dict[str, Optional[str]]]:
    params = { "view": CATALOGUE_VIEW, "pageSize": 100, "fields[]": [CATALOGUE_ISRC_FIELD, CATALOGUE_ARTIST_FIELD, CATALOGUE_TITLE_FIELD] }
    rows = at_paginate(CATALOGUE_TABLE, params)
    out: Dict[str, Dict[str, Optional[str]]] = {}
    for rec in rows:
        f = rec.get("fields", {})
        isrc = (f.get(CATALOGUE_ISRC_FIELD) or "").strip().upper()
        if not isrc: continue
        out[isrc] = { "air_id": rec["id"], "artist": (f.get(CATALOGUE_ARTIST_FIELD) or [None])[0], "title": f.get(CATALOGUE_TITLE_FIELD) }
    return out

def db_conn():
    if not DATABASE_URL: raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def db_ensure_platform(cur, platform: str = "spotify"):
    cur.execute("INSERT INTO platform_dim(platform) VALUES (%s) ON CONFLICT DO NOTHING", (platform,))

def db_upsert_track(cur, isrc: str, artist: Optional[str] = None, title: Optional[str] = None) -> str:
    cur.execute("INSERT INTO track_dim(isrc, artist, title) VALUES (%s, %s, %s) ON CONFLICT (isrc) DO UPDATE SET artist = COALESCE(EXCLUDED.artist, track_dim.artist), title  = COALESCE(EXCLUDED.title,  track_dim.title) RETURNING track_uid", (isrc, artist, title))
    return cur.fetchone()["track_uid"]

def db_upsert_stream(cur, platform: str, track_uid: str, day_iso: str, playcount: int):
    cur.execute("INSERT INTO streams(platform, track_uid, stream_date, playcount) VALUES (%s, %s, %s, %s) ON CONFLICT (platform, track_uid, stream_date) DO UPDATE SET playcount = EXCLUDED.playcount", (platform, track_uid, day_iso, playcount))

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

#
# ────────────────────────────────────────────────────────────────────────────────
# Spotify helpers for streams
#
# ────────────────────────────────────────────────────────────────────────────────
# 🚀 REPLACED THE OLD TOKEN FUNCTION WITH THIS RELIABLE ONE
def get_search_token() -> str:
    """
    Gets a new Spotify access token using the Client Credentials Flow.
    This is more reliable for server-to-server calls.
    """
    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError("SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET must be set.")
    
    logger.info("Requesting new Spotify search token using client credentials (through proxy)...")
    
    r = _spotify_request_with_retries(
        "post",
        SPOTIFY_TOKEN_URL,
        auth=(CLIENT_ID, CLIENT_SECRET),
        data={"grant_type": "client_credentials"},
        timeout=30
    )
    
    token = r.json().get("access_token")
    if not token:
        # Add more detail to the error log
        logger.error(f"Failed to get access_token. Status: {r.status_code}, Response: {r.text}")
        raise RuntimeError("Failed to get access_token from Spotify.")
        
    logger.info("Successfully obtained new Spotify search token.")
    return token

def search_track(isrc: str, bearer: str) -> Optional[Tuple[str, str, str, Optional[str]]]:
    r = _spotify_request_with_retries("get", SPOTIFY_SEARCH_URL,
                                      headers={"Authorization": f"Bearer {bearer}"},
                                      params={"q": f"isrc:{isrc}", "type": "track", "limit": 5},
                                      timeout=60)
    items = r.json().get("tracks", {}).get("items", [])
    if not items: return None
    best = items[0]
    track_id, album_id, track_name = best.get("id"), best.get("album", {}).get("id"), best.get("name")
    artists = [a.get("name") for a in (best.get("artists") or []) if a.get("name")]
    artists_joined = " & ".join(artists) if artists else None
    if not (track_id and album_id): return None
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
        await page.goto("https://open.spotify.com/")
        try:
            return await asyncio.wait_for(fut, timeout=90)
        finally:
            await browser.close()

def fetch_album(album_id: str, web_token: str, client_token: Optional[str]) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {web_token}", "User-Agent": USER_AGENT, "content-type": "application/json"}
    if client_token: headers["Client-Token"] = client_token
    body = { "operationName": OPERATION_NAME, "variables": {**CAPTURED_VARS, "uri": f"spotify:album:{album_id}"}, "extensions": {"persistedQuery": {"version": 1, "sha256Hash": PERSISTED_HASH}}, }
    try:
        r = _spotify_request_with_retries("post", SPOTIFY_PATHFINDER_URL, headers=headers, json=body, timeout=30)
        response_json = r.json()
        if not response_json.get("data"):
            streams_logger.warning(f"Received empty 'data' from Spotify for album {album_id}. Likely a proxy/IP block.")
            return {}
        return response_json
    except requests.exceptions.RequestException:
        return {}

#
# ────────────────────────────────────────────────────────────────────────────────
# MAIN WORKER: Track Streams
#
# ────────────────────────────────────────────────────────────────────────────────
async def run_once(day_override: Optional[str] = None, attempt_idx: int = 1, output_target: str = OUTPUT_TARGET) -> Dict[str, Any]:
    day_iso = day_override or (date.today() - timedelta(days=1)).isoformat()
    
    catalogue = catalogue_index()
    cat_isrcs = list(catalogue.keys())
    
    streams_logger.info("starting run: tracks=%d output=%s attempt=%d date=%s", len(cat_isrcs), output_target, attempt_idx, day_iso)

    try:
        # 🟢 UPDATED TO USE THE NEW, CORRECT FUNCTION
        search_token = get_search_token()
        web_token, client_token = await sniff_tokens()
    except Exception as e:
        streams_logger.error(f"Failed to get auth tokens, cannot proceed. Error: {e}")
        error_message = f"ALERT: Spotify stream run failed during token setup. Error: {e}"
        send_telegram_alert(error_message)
        return {"error": "Token acquisition failed", "details": str(e)}

    processed, errors, sum_pc = 0, 0, 0
    records_to_process = []
    
    for i, isrc in enumerate(cat_isrcs):
        if (i+1) % 50 == 0:
            streams_logger.info("progress: %d/%d", i+1, len(cat_isrcs))
        
        playcount = None
        try:
            track_info = search_track(isrc, search_token)
            if not track_info:
                time.sleep(0.1)
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
                        break
            
            cat_data = catalogue.get(isrc, {})
            records_to_process.append({
                "isrc": isrc, "artist": cat_data.get("artist") or api_artist,
                "title": cat_data.get("title") or api_title, "playcount": playcount,
            })
            processed += 1
            if playcount is not None: sum_pc += playcount
            time.sleep(0.2)
        
        except requests.exceptions.RequestException as e:
            streams_logger.warning(f"Skipping ISRC {isrc} due to network/proxy error: {e}")
            errors += 1
            continue
        except Exception as e:
            streams_logger.error("error processing ISRC=%s: %s", isrc, e, exc_info=True)
            errors += 1

    conn = None
    try:
        conn = db_conn()
        with conn.cursor() as cur:
            cur.execute("INSERT INTO platform_dim(platform) VALUES ('spotify') ON CONFLICT DO NOTHING")
            for record in records_to_process:
                if record["playcount"] is not None:
                    try:
                        track_uid = db_upsert_track(cur, record["isrc"], record["artist"], record["title"])
                        db_upsert_stream(cur, "spotify", track_uid, day_iso, record["playcount"])
                    except Exception as e:
                        streams_logger.error(f"[postgres] error on ISRC={record['isrc']}: {e}")
                        errors += 1
        conn.commit()
        
        with conn.cursor() as cur:
            daily_delta_pg = db_catalogue_delta_for_day(cur, day_iso)
    except psycopg2.Error as e:
        streams_logger.exception(f"[postgres] transaction failed: {e}")
        if conn: conn.rollback()
        daily_delta_pg = 0
    finally:
        if conn: conn.close()

    stats = {
        "processed": processed, "errors": errors, "date": day_iso,
        "total_lifetime_streams_found": sum_pc,
        "total_daily_streams_gained": daily_delta_pg,
    }
    log_summary = (
        f"Run completed for {day_iso}. "
        f"Processed: {processed}, Errors: {errors}. "
        f"Total streams found: {sum_pc:,}. Daily gain: {daily_delta_pg:,}."
    )
    streams_logger.info(log_summary)
    
    if processed > 0 and sum_pc == 0:
        error_message = f"ALERT: Spotify stream run for {day_iso} completed but found 0 total streams. Please check the system."
        send_telegram_alert(error_message)

    return stats

#
# ────────────────────────────────────────────────────────────────────────────────
# Scheduler & Flask App
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
                try: return await run_once(attempt_idx=i+1)
                except Exception as e:
                    scheduler_logger.exception("run_once failed (attempt %d/3): %s", i + 1, e)
                    if i < 2: await asyncio.sleep(60 * (i + 1))
            return {"error": "failed after 3 attempts"}
        
        _run_task = asyncio.create_task(run_with_retries())
        await _run_task

    next_run_time = time.time()
    while not _RUNNING.is_set():
        if time.time() >= next_run_time:
            scheduler_logger.info("tick → running sync()")
            try:
                asyncio.run(sync())
            except Exception as e:
                scheduler_logger.error("loop error: %s", e, exc_info=True)
            next_run_time = time.time() + delay_secs
        
        time.sleep(30)
    scheduler_logger.info("scheduler loop exiting")


app = Flask(__name__)

@app.route("/")
def index():
    return jsonify({ "status": "ok", "scheduler_enabled": ENABLE_SCHEDULER })

# ... (The rest of your Flask routes can remain the same) ...

if __name__ == "__main__":
    if ENABLE_SCHEDULER:
        threading.Thread(target=_schedule_loop, daemon=True).start()
    
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
