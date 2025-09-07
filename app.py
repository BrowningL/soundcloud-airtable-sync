import os
import time
import asyncio
import threading
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import logging
import requests
from flask import Flask, jsonify, request, abort
from playwright.async_api import async_playwright

# Postgres
import psycopg2
from psycopg2.extras import RealDictCursor

# ────────────────────────────────────────────────────────────────────────────────
# LOGGING (Railway captures stdout/stderr and python logging)
# ────────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger("railway")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger.setLevel(logging.INFO)

# Try to import the catalogue health worker with defensive logging
try:
    from catalogue_health.catalogue_health import run_catalogue_health as ch_run
    logger.info("[catalogue_health] module import OK")
except Exception as e:
    ch_run = None  # type: ignore
    logger.exception("[catalogue_health] import failed: %s", e)

# ────────────────────────────────────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────────────────────────────────────
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY") or os.getenv("AT_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID") or os.getenv("AT_BASE_ID") or "appAmLhYAVcmKmRC3"
if not AIRTABLE_API_KEY or not AIRTABLE_API_KEY.startswith("pat"):
    raise RuntimeError("Set AIRTABLE_API_KEY to a valid Airtable Personal Access Token (starts with 'pat').")

DATABASE_URL = os.getenv("DATABASE_URL")  # e.g. postgresql://railway:***@.../timeseriesdb
OUTPUT_TARGET = os.getenv("OUTPUT_TARGET", "postgres").lower()  # 'postgres' | 'airtable' | 'both'
AUTOMATION_TOKEN = os.getenv("AUTOMATION_TOKEN")  # optional: set to protect write routes
LOCAL_TZ = os.getenv("LOCAL_TZ", "Europe/London")

# ----- Catalogue (ISRC list) -----
CATALOGUE_TABLE = os.getenv("CATALOGUE_TABLE", "Catalogue")
CATALOGUE_VIEW = os.getenv("CATALOGUE_VIEW", "Inner Catalogue")
CATALOGUE_ISRC_FIELD = os.getenv("CATALOGUE_ISRC_FIELD", "ISRC")
CATALOGUE_ARTIST_FIELD = os.getenv("CATALOGUE_ARTIST_FIELD", "Artist")  # lookup/rollup or text
CATALOGUE_TITLE_FIELD  = os.getenv("CATALOGUE_TITLE_FIELD",  "Track Title")   # lookup/rollup or text

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
FOLLOWERS_LINK_FIELD = os.getenv("FOLLOWERS_LINK_FIELD", "Playlist")  # linked → Playlists
FOLLOWERS_DATE_FIELD = os.getenv("FOLLOWERS_DATE_FIELD", "Date")
FOLLOWERS_COUNT_FIELD = os.getenv("FOLLOWERS_COUNT_FIELD", "Followers")
FOLLOWERS_DELTA_FIELD = os.getenv("FOLLOWERS_DELTA_FIELD", "Delta")
FOLLOWERS_ALLOW_NEGATIVE = (os.getenv("FOLLOWERS_ALLOW_NEGATIVE", "true").lower() in ("1", "true", "yes"))

# Spotify creds
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "YOUR_SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "YOUR_SPOTIFY_CLIENT_SECRET")

# Spotify web GraphQL for streams (unchanged)
OPERATION_NAME = "getAlbum"
PERSISTED_HASH = "97dd13a1f28c80d66115a13697a7ffd94fe3bebdb94da42159456e1d82bfee76"
CAPTURED_VARS = {"locale": "", "offset": 0, "limit": 50}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
PATHFINDER_HOSTS = [
    "api-partner.spotify.com",
    "spclient.wg.spotify.com",
    "gew1-spclient.spotify.com",
    "guc3-spclient.spotify.com",
]

airtable_sleep = float(os.getenv("AT_SLEEP", "0.2"))
spotify_sleep = float(os.getenv("SPOTIFY_SLEEP", "0.15"))

# ── Lag config (simple hard floor + caps + scheduler) ───────────────────────────
LAG_MIN_TOTAL = int(os.getenv("LAG_MIN_TOTAL", "20000"))  # catalogue floor per completed day
CAP_CHECKPOINT_RATIO = float(os.getenv("CAP_CHECKPOINT_RATIO", "0.30"))  # ≤30% of target per checkpoint
CAP_DAILY_RATIO      = float(os.getenv("CAP_DAILY_RATIO", "0.60"))       # ≤60% of target per calendar day
ENABLE_SCHEDULER     = os.getenv("ENABLE_SCHEDULER", "true").lower() in ("1","true","yes")
SCHEDULE_EVERY_HOURS = int(os.getenv("SCHEDULE_EVERY_HOURS", "6"))

# ────────────────────────────────────────────────────────────────────────────────
# Airtable helpers
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
            "title":  _norm_lookup(f.get(CATALOGUE_TITLE_FIELD)),
        }
    return out

# ────────────────────────────────────────────────────────────────────────────────
# Postgres helpers
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

# ────────────────────────────────────────────────────────────────────────────────
# Track Playcounts helpers (Airtable)
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
        f"SEARCH('{_q(isrc_code)}', ARRAYJOIN({{{PLAYCOUNTS_LINK_FIELD}}}))",
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

# ────────────────────────────────────────────────────────────────────────────────
# Playlists master (Airtable list → which playlists to track)
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

# ────────────────────────────────────────────────────────────────────────────────
# Spotify helpers for streams
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

def get_search_token() -> str:
    r = requests.post("https://accounts.spotify.com/api/token",
                      data={"grant_type": "client_credentials"},
                      auth=(CLIENT_ID, CLIENT_SECRET), timeout=60)
    r.raise_for_status()
    return r.json()["access_token"]

def search_track(isrc: str, bearer: str) -> Optional[Tuple[str, str, str, Optional[str]]]:
    """
    Return (track_id, album_id, track_name, artists_joined) for the given ISRC,
    or None if not found.
    """
    r = requests.get("https://api.spotify.com/v1/search",
                     headers={"Authorization": f"Bearer {bearer}"},
                     params={"q": f"isrc:{isrc}", "type": "track", "limit": 5},
                     timeout=60)
    if r.status_code != 200:
        return None
    items = r.json().get("tracks", {}).get("items", [])
    if not items:
        return None

    # Prefer exact ISRC match if available
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

async def sniff_tokens() -> Tuple[str, Optional[str]]:  # unchanged
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
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
        await page.goto("https://open.spotify.com/album/2noRn2Aes5aoNVsU6iWThc")
        try:
            return await asyncio.wait_for(fut, timeout=15)
        finally:
            await browser.close()

def fetch_album(album_id: str, web_token: str, client_token: Optional[str]) -> Dict[str, Any]:
    sess = requests.Session()
    headers = {"Authorization": f"Bearer {web_token}", "User-Agent": USER_AGENT, "content-type": "application/json"}
    if client_token:
        headers["Client-Token"] = client_token
    body = {
        "operationName": OPERATION_NAME,
        "variables": {**CAPTURED_VARS, "uri": f"spotify:album:{album_id}"},
        "extensions": {"persistedQuery": {"version": 1, "sha256Hash": PERSISTED_HASH}},
    }
    for host in PATHFINDER_HOSTS:
        try:
            r = sess.post(f"https://{host}/pathfinder/v2/query", headers=headers, json=body, timeout=30)
            if r.status_code == 200:
                return r.json()
        except Exception:
            continue
    return {}

# ────────────────────────────────────────────────────────────────────────────────
# Generic Airtable delta recompute (unchanged)
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

# ────────────────────────────────────────────────────────────────────────────────
# Lag schema & catalogue totals (DB-only; idempotent)
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
    """
    Sum of positive per-track increments on day_iso.
    delta(track, day) = max(0, pc(day) - last_pc_before(day))
    """
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
    """
    All past, unfinalized days whose catalogue delta < LAG_MIN_TOTAL, oldest first.
    """
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
    """
    Enforce per-checkpoint and per-day caps for a given anchor.
    """
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
    """
    Returns list of (track_uid, prev_pc, inc_today). inc_today = max(0, pc_today - prev_pc).
    """
    cur.execute("""
        WITH today AS (
          SELECT s.track_uid, s.playcount AS pc_today
          FROM streams s
          WHERE s.platform='spotify' AND s.stream_date=%s
        ),
        prev AS (
          SELECT t.track_uid,
                 (SELECT s2.playcount
                  FROM streams s2
                  WHERE s2.platform='spotify' AND s2.track_uid=t.track_uid AND s2.stream_date < %s
                  ORDER BY s2.stream_date DESC LIMIT 1) AS pc_prev
          FROM track_dim t
        )
        SELECT t.track_uid,
               COALESCE(p.pc_prev,0) AS pc_prev,
               GREATEST(0, COALESCE(t.pc_today,0) - COALESCE(p.pc_prev,0)) AS inc_today
        FROM today t
        JOIN prev p USING (track_uid)
        WHERE COALESCE(t.pc_today,0) > COALESCE(p.pc_prev,0);
    """, (today_iso, today_iso))
    rows = cur.fetchall()
    return [(r["track_uid"], int(r["pc_prev"]), int(r["inc_today"])) for r in rows]

def daily_housekeeping(cur, today_iso: str):
    """
    Idempotent: safe to call every run.
    - Ensure schema
    - Reset lag_credits.moved_today at local midnight (UTC acceptable)
    - Auto-finalize days strictly older than D-2 that already meet the floor
    """
    db_ensure_lag_schema(cur)
    cur.execute("UPDATE lag_credits SET moved_today = 0, updated_at=now() WHERE day < CURRENT_DATE")
    cur.execute("""
        UPDATE daily_totals
        SET finalized=true, updated_at=now()
        WHERE day < CURRENT_DATE - INTERVAL '2 days' AND finalized=false AND total_delta >= %s
    """, (LAG_MIN_TOTAL,))

def reattribute_increments_to_queue(cur, today_iso: str) -> Dict[str, Any]:
    """
    Oldest-first fill of any number of lag days. Only moves from TODAY → ANCHOR(s).
    """
    # Update yesterday snapshot if we have rows for it
    cur.execute("SELECT CURRENT_DATE - INTERVAL '1 day' AS yday")
    yday = cur.fetchone()["yday"].date().isoformat()
    cur.execute("SELECT 1 FROM streams WHERE platform='spotify' AND stream_date=%s LIMIT 1", (yday,))
    if cur.fetchone():
        y_delta = db_catalogue_delta_for_day(cur, yday)
        db_upsert_daily_total(cur, yday, y_delta, finalized=False)

    queue = db_get_lag_queue(cur, today_iso)
    if not queue:
        # still record today's snapshot
        t_delta = db_catalogue_delta_for_day(cur, today_iso)
        db_upsert_daily_total(cur, today_iso, t_delta, finalized=False)
        return {"moved_total": 0, "anchors": []}

    triples = db_today_increments(cur, today_iso)
    if not triples:
        t_delta = db_catalogue_delta_for_day(cur, today_iso)
        db_upsert_daily_total(cur, today_iso, t_delta, finalized=False)
        return {"moved_total": 0, "anchors": queue}

    remaining_by_track = {tid: inc for (tid, _prev, inc) in triples}
    prev_by_track      = {tid: prev for (tid, prev, _inc) in triples}
    moved_overall = 0

    for anchor_day in queue:
        cur.execute("SELECT total_delta FROM daily_totals WHERE day=%s", (anchor_day,))
        row = cur.fetchone()
        current = int(row["total_delta"]) if row else 0
        need = max(0, LAG_MIN_TOTAL - current)
        if need <= 0:
            db_mark_finalized_if_ready(cur, anchor_day)
            continue

        allowed = _cap_amount_for_anchor(cur, anchor_day, need)
        if allowed <= 0:
            continue

        available_today = sum(remaining_by_track.values())
        if available_today <= 0:
            break

        move_amount = min(allowed, available_today)
        to_move = move_amount

        # proportional by remaining inc, monotonic guard
        while to_move > 0:
            total_rem = sum(remaining_by_track.values())
            if total_rem <= 0:
                break
            for tid, rem in list(remaining_by_track.items()):
                if rem <= 0 or to_move <= 0:
                    continue
                share = int(round(rem * (to_move / total_rem)))
                share = max(0, min(share, remaining_by_track[tid]))
                if share == 0:
                    continue

                # Decrease today's pc (not below prev)
                new_today_pc = prev_by_track[tid] + (remaining_by_track[tid] - share)
                cur.execute("""
                    UPDATE streams SET playcount=%s
                    WHERE platform='spotify' AND track_uid=%s AND stream_date=%s
                """, (new_today_pc, tid, today_iso))

                # Increase anchor day's pc (upsert)
                cur.execute("""
                    SELECT playcount FROM streams
                    WHERE platform='spotify' AND track_uid=%s AND stream_date=%s
                """, (tid, anchor_day))
                row_a = cur.fetchone()
                if row_a:
                    anchor_pc_base = int(row_a["playcount"] or 0)
                else:
                    cur.execute("""
                        SELECT playcount FROM streams
                        WHERE platform='spotify' AND track_uid=%s AND stream_date<%s
                        ORDER BY stream_date DESC LIMIT 1
                    """, (tid, anchor_day))
                    rp = cur.fetchone()
                    anchor_pc_base = int(rp["playcount"] or 0) if rp else 0

                new_anchor_pc = anchor_pc_base + share
                cur.execute("""
                    INSERT INTO streams(platform, track_uid, stream_date, playcount)
                    VALUES ('spotify', %s, %s, %s)
                    ON CONFLICT (platform, track_uid, stream_date)
                    DO UPDATE SET playcount=EXCLUDED.playcount
                """, (tid, anchor_day, new_anchor_pc))

                remaining_by_track[tid] -= share
                moved_overall += share
                to_move -= share

        # Update anchor totals/credits + finalize check
        new_total = current + min(move_amount, available_today)
        db_upsert_daily_total(cur, anchor_day, new_total, finalized=False)
        _bump_lag_credits(cur, anchor_day, min(move_amount, available_today))
        db_mark_finalized_if_ready(cur, anchor_day)

    # Update today's snapshot (not finalized)
    t_delta = db_catalogue_delta_for_day(cur, today_iso)
    db_upsert_daily_total(cur, today_iso, t_delta, finalized=False)
    return {"moved_total": moved_overall, "anchors": queue}

# ────────────────────────────────────────────────────────────────────────────────
# Streams daily sync (with logging)  >>> RETRY-ON-ZERO UPGRADE
# ────────────────────────────────────────────────────────────────────────────────
async def sync(max_retries: int = 1, retry_sleep_sec: float = 3.0):
    """
    Run the sync. If the run looks like a silent failure, retry once:
      • Primary trigger: sum of today's deltas == 0
      • Fallback trigger: sum of fetched playcounts == 0
    On retry, refresh tokens and clear album cache. Second pass overwrites zeros.
    """
    cat = catalogue_index()  # { ISRC: {air_id, artist, title} }
    order = list(cat.keys())
    linkmap = {k: v["air_id"] for k, v in cat.items()}
    today_iso = date.today().isoformat()

    async def run_once(attempt_idx: int):
        nonlocal today_iso
        logger.info(f"[streams] starting run: tracks={len(order)} output={OUTPUT_TARGET} attempt={attempt_idx+1}")

        web_tok, cli_tok = await sniff_tokens()
        search_tok = get_search_token()
        cache: Dict[str, Dict[str, Any]] = {}

        conn = None
        cur = None
        if OUTPUT_TARGET in ("postgres", "both"):
            conn = db_conn()
            cur = conn.cursor()
            db_ensure_platform(cur, "spotify")

        errors = 0
        processed = 0
        sum_pc = 0
        sum_delta_like = 0  # computed the same way as Airtable delta logic

        try:
            for isrc in order:
                try:
                    tr = search_track(isrc, search_tok)  # (tid, aid, tname, artists) or None
                    if not tr:
                        # No match → treat as 0
                        pc = 0
                        prev = prev_count_by_isrc(isrc, today_iso)
                        if prev is not None and prev > 0:
                            # delta-like stays 0
                            pass

                        if OUTPUT_TARGET in ("airtable", "both"):
                            upsert_count(linkmap[isrc], isrc, today_iso, pc)
                        if OUTPUT_TARGET in ("postgres", "both"):
                            meta = cat.get(isrc, {})
                            track_uid = db_upsert_track(cur, isrc, meta.get("artist"), meta.get("title"))
                            db_upsert_stream(cur, "spotify", track_uid, today_iso, pc)

                        sum_pc += pc
                        processed += 1
                        continue

                    tid, aid, sp_title, sp_artists = tr
                    if aid not in cache:
                        time.sleep(spotify_sleep)
                        cache[aid] = fetch_album(aid, web_tok, cli_tok)

                    # Find playcount for that track on the album page
                    pc = 0
                    js = cache[aid]
                    for it in js.get("data", {}).get("albumUnion", {}).get("tracksV2", {}).get("items", []):
                        track = it.get("track", {})
                        cand = (track.get("uri", "") or "").split(":")[-1] if track.get("uri") else None
                        if tid in (track.get("id"), cand):
                            val = track.get("playcount") or 0
                            try:
                                pc = int(str(val).replace(",", ""))
                            except Exception:
                                pc = 0
                            break

                    # delta-like (prev from Airtable history if present)
                    prev = prev_count_by_isrc(isrc, today_iso)
                    if prev is not None and prev > 0 and pc > 0:
                        d = pc - prev
                        if d > 0:
                            sum_delta_like += d

                    meta = cat.get(isrc, {})
                    artist = meta.get("artist") or sp_artists
                    title  = meta.get("title")  or sp_title

                    if OUTPUT_TARGET in ("airtable", "both"):
                        upsert_count(linkmap[isrc], isrc, today_iso, pc)
                    if OUTPUT_TARGET in ("postgres", "both"):
                        track_uid = db_upsert_track(cur, isrc, artist, title)
                        db_upsert_stream(cur, "spotify", track_uid, today_iso, pc)

                    sum_pc += pc
                    processed += 1

                except Exception as e:
                    errors += 1
                    logger.exception(f"[streams] error processing ISRC={isrc}: {e}")

            if conn:
                conn.commit()
        finally:
            if cur: cur.close()
            if conn: conn.close()

        logger.info(f"[streams] completed: processed={processed} errors={errors} date={today_iso} "
                    f"output={OUTPUT_TARGET} sum_pc={sum_pc} sum_delta_like={sum_delta_like} attempt={attempt_idx+1}")
        return {"processed": processed, "errors": errors, "sum_pc": sum_pc, "sum_delta_like": sum_delta_like}

    # First attempt
    stats = await run_once(attempt_idx=0)

    # Decide retry
    should_retry = False
    reason = None
    if stats["sum_delta_like"] == 0:
        should_retry = True
        reason = "sum_delta_like==0"
    elif stats["sum_pc"] == 0:
        should_retry = True
        reason = "sum_pc==0"

    if should_retry and max_retries > 0:
        logger.warning(f"[streams] retry triggered ({reason}); sleeping {retry_sleep_sec}s then refreshing tokens…")
        time.sleep(retry_sleep_sec)
        stats2 = await run_once(attempt_idx=1)
        if stats2["sum_delta_like"] == 0 and stats2["sum_pc"] == 0:
            logger.error("[streams] hard-zero after retry (both sum_delta_like and sum_pc are 0).")
        else:
            logger.info("[streams] retry succeeded (non-zero signal detected).")

    # ── Lag reconciliation (DB-only; restart/idempotent-safe)
    try:
        today_iso = date.today().isoformat()
        with db_conn() as conn, conn.cursor() as cur:
            daily_housekeeping(cur, today_iso)
            res = reattribute_increments_to_queue(cur, today_iso)
            conn.commit()
        logger.info(f"[lag] moved={res['moved_total']} anchors={res['anchors']}")
    except Exception as e:
        logger.exception("[lag] reconciliation failed: %s", e)

# ────────────────────────────────────────────────────────────────────────────────
# Followers backfill / today (with logging)
# ────────────────────────────────────────────────────────────────────────────────
def backfill_playlist_followers_all(platform: str = "spotify") -> int:
    idx = playlists_index_from_airtable()
    params = {"pageSize": 100, "sort[0][field]": FOLLOWERS_DATE_FIELD, "sort[0][direction]": "asc"}
    rows = at_paginate(FOLLOWERS_TABLE, params)

    inserted = 0
    with db_conn() as conn, conn.cursor() as cur:
        db_ensure_platform(cur, platform)
        for r in rows:
            f = r.get("fields", {})
            links = f.get(FOLLOWERS_LINK_FIELD) or []
            if not links:
                continue
            meta = idx.get(links[0])
            if not meta:
                continue
            urn = meta["playlist_id_urn"]
            day_iso = f.get(FOLLOWERS_DATE_FIELD)
            try:
                followers = int(f.get(FOLLOWERS_COUNT_FIELD, 0) or 0)
            except Exception:
                followers = 0
            db_upsert_playlist_followers(cur, platform, urn, day_iso, followers, meta.get("name"))
            inserted += 1
        conn.commit()
    logger.info(f"[followers/backfill→db] completed: inserted_or_updated={inserted}")
    return inserted

def get_client_bearer() -> str:
    r = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        auth=(CLIENT_ID, CLIENT_SECRET),
        timeout=60,
    )
    if not r.ok:
        raise RuntimeError(f"spotify_token_error: {r.status_code} {r.text}")
    return r.json()["access_token"]

def fetch_playlist_followers_spotify(plain_id: str, bearer: str) -> Optional[int]:
    r = requests.get(
        f"https://api.spotify.com/v1/playlists/{plain_id}",
        headers={"Authorization": f"Bearer {bearer}"},
        params={"fields": "followers.total,name"},
        timeout=30,
    )
    if r.status_code != 200:
        return None
    return int(r.json().get("followers", {}).get("total", 0) or 0)

def run_followers_today(platform: str = "spotify", tzkey: Optional[str] = None) -> Dict[str, Any]:
    idx = playlists_index_from_airtable()
    if not idx:
        logger.warning("[followers] aborted: no playlists found in Airtable source")
        return {"inserted": 0, "skipped": 0, "reason": "no_playlists"}

    try:
        bearer = get_client_bearer()
    except Exception as e:
        logger.exception(f"[followers] failed to get bearer: {e}")
        raise

    today = today_iso_local()

    inserted = 0
    skipped = 0
    with db_conn() as conn, conn.cursor() as cur:
        db_ensure_platform(cur, platform)
        for meta in idx.values():
            urn = meta["playlist_id_urn"]
            plain = urn_to_plain_id(urn)
            followers = fetch_playlist_followers_spotify(plain, bearer)
            if followers is None:
                skipped += 1
                logger.warning(f"[followers] skip pid={plain} reason=api_error_or_rate_limit")
                continue
            db_upsert_playlist_followers(cur, platform, urn, today, followers, meta.get("name"))
            inserted += 1
        conn.commit()

    logger.info(f"[followers] completed: playlists={len(idx)} inserted={inserted} skipped={skipped} date={today}")
    return {"inserted": inserted, "skipped": skipped, "date": today}

# ────────────────────────────────────────────────────────────────────────────────
# Flask endpoints (with logging)
# ────────────────────────────────────────────────────────────────────────────────
app = Flask(__name__)

@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "base": AIRTABLE_BASE_ID,
        "output_target": OUTPUT_TARGET,
        "has_spotify_creds": _has_spotify_creds()
    })

@app.get("/airtable/ping")
def airtable_ping():
    try:
        r = requests.get(at_url(PLAYCOUNTS_TABLE), headers=at_headers(), params={"pageSize": 1}, timeout=30)
        ok = r.ok
        body = r.json() if ok else {"error": r.text}
        return jsonify({"ok": ok, "status": r.status_code, "table": PLAYCOUNTS_TABLE, "base": AIRTABLE_BASE_ID, "body": body}), (200 if ok else 502)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/db/ping")
def db_ping():
    try:
        with db_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok")
            row = cur.fetchone()
        return jsonify({"ok": True, "row": row}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/diag/playlists")
def diag_playlists():
    idx = playlists_index_from_airtable()
    return jsonify({"ok": True, "count": len(idx), "sample": list(idx.values())[:5]}), 200

@app.get("/diag/spotify_token")
def diag_spotify_token():
    try:
        tok = get_client_bearer()
        return jsonify({"ok": True, "token_prefix": tok[:12]}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/diag/spotify")
def diag_spotify():
    pid = request.args.get("pid")
    if not pid:
        return jsonify({"ok": False, "error": "pid required"}), 400
    try:
        bearer = get_client_bearer()
        val = fetch_playlist_followers_spotify(pid, bearer)
        return jsonify({"ok": True, "followers": val}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.post("/run")
def run_endpoint():
    _check_token()
    async_flag = request.args.get("async", "0").lower() in ("1", "true", "yes")
    if async_flag:
        logger.info("[streams] async start requested")
        threading.Thread(target=lambda: asyncio.run(sync()), daemon=True).start()
        return jsonify({"status": "started"}), 202
    else:
        logger.info("[streams] sync start")
        try:
            asyncio.run(sync())
            logger.info("[streams] sync completed successfully")
            return jsonify({"status": "completed"}), 200
        except Exception as e:
            logger.exception(f"[streams] sync failed: {e}")
            return jsonify({"status": "failed", "error": str(e)}), 500

@app.post("/backfill")
def backfill_endpoint():
    _check_token()
    try:
        changed = backfill_deltas_for_all_tracks()
        logger.info(f"[backfill/streams] completed: changed={changed}")
        return jsonify({"status": "backfilled_playcounts", "changed": changed}), 200
    except Exception as e:
        logger.exception(f"[backfill/streams] failed: {e}")
        return jsonify({"status": "failed", "error": str(e)}), 500

@app.post("/backfill_followers")
def backfill_followers_endpoint():
    _check_token()
    try:
        changed = backfill_deltas_for_followers()
        logger.info(f"[backfill/followers] completed: changed={changed}")
        return jsonify({"status": "backfilled_followers", "changed": changed}), 200
    except Exception as e:
        logger.exception(f"[backfill/followers] failed: {e}")
        return jsonify({"status": "failed", "error": str(e)}), 500

@app.post("/run_catalogue_health")
def run_catalogue_health_endpoint():
    _check_token()
    if ch_run is None:
        logger.error("[catalogue_health] cannot run: module import failed previously")
        return jsonify({"ok": False, "error": "module_import_failed"}), 500

    async_flag = (request.args.get("async", "0").lower() in ("1", "true", "yes"))
    limit = request.args.get("limit", type=int)  # optional
    dry = (request.args.get("dry_run", "0").lower() in ("1", "true", "yes"))

    def _job():
        try:
            logger.info("[catalogue_health] ▶ start | limit=%s dry_run=%s", limit, dry)
            res = ch_run(limit_override=limit, dry_run_override=dry)
            logger.info("[catalogue_health] ✅ finish | %s", res)
        except Exception as e:
            logger.exception("[catalogue_health] ❌ failed: %s", e)

    if async_flag:
        threading.Thread(target=_job, daemon=True).start()
        return jsonify({"ok": True, "status": "started"}), 202

    try:
        res = ch_run(limit_override=limit, dry_run_override=dry)
        logger.info("[catalogue_health] ✅ finish | %s", res)
        return jsonify({"ok": True, **res}), 200
    except Exception as e:
        logger.exception("[catalogue_health] ❌ failed: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500

# ────────────────────────────────────────────────────────────────────────────────
# Backfill Streams (Airtable → Postgres)  (UPDATED to upsert artist/title)
# ────────────────────────────────────────────────────────────────────────────────
def backfill_airtable_to_postgres(days: Optional[str] = None) -> int:
    # Build ISRC → (artist,title) map up front
    cat = catalogue_index()  # {ISRC: {air_id, artist, title}}

    recs = at_paginate(CATALOGUE_TABLE, {"view": CATALOGUE_VIEW, "pageSize": 100, "fields[]": CATALOGUE_ISRC_FIELD})
    cat_id_to_isrc = {r["id"]: (r["fields"].get(CATALOGUE_ISRC_FIELD) or "").strip().upper()
                      for r in recs if r.get("fields", {}).get(CATALOGUE_ISRC_FIELD)}

    all_data = False
    d_val: Optional[int] = None
    if days is None:
        all_data = True
    else:
        try:
            d_val = int(days)
            all_data = (d_val <= 0)
        except Exception:
            all_data = str(days).lower() in ("all", "everything", "full")

    params = {"pageSize": 100, "sort[0][field]": PLAYCOUNTS_DATE_FIELD, "sort[0][direction]": "asc"}
    if not all_data and d_val is not None and d_val > 0:
        params["filterByFormula"] = f"IS_AFTER({{{PLAYCOUNTS_DATE_FIELD}}}, DATEADD(TODAY(), -{d_val}, 'days'))"

    rows = at_paginate(PLAYCOUNTS_TABLE, params)

    inserted = 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            db_ensure_platform(cur, "spotify")
            for r in rows:
                f = r.get("fields", {})
                links = f.get(PLAYCOUNTS_LINK_FIELD) or []
                if not links:
                    continue
                isrc = (cat_id_to_isrc.get(links[0]) or "").strip().upper()
                if not isrc:
                    continue
                day_iso = f.get(PLAYCOUNTS_DATE_FIELD)
                try:
                    count = int(f.get(PLAYCOUNTS_COUNT_FIELD, 0) or 0)
                except Exception:
                    count = 0

                meta = cat.get(isrc, {})
                track_uid = db_upsert_track(cur, isrc, meta.get("artist"), meta.get("title"))
                db_upsert_stream(cur, "spotify", track_uid, day_iso, count)
                inserted += 1
        conn.commit()
    logger.info(f"[backfill/airtable→postgres] completed: inserted={inserted} days={days}")
    return inserted

# ────────────────────────────────────────────────────────────────────────────────
# 6h scheduler (aligned 00/06/12/18) with advisory-lock singleton
# ────────────────────────────────────────────────────────────────────────────────
def _seconds_until_next_tick(now: datetime, step_hours: int = 6) -> int:
    block = ((now.hour // step_hours) + 1) * step_hours
    next_dt = now.replace(minute=0, second=0, microsecond=0)
    if block >= 24:
        next_dt = (next_dt.replace(hour=0) + timedelta(days=1))
    else:
        next_dt = next_dt.replace(hour=block)
    return max(1, int((next_dt - now).total_seconds()))

def _try_advisory_lock(conn, key_bigint: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s) AS ok", (key_bigint,))
        return bool(cur.fetchone()["ok"])

def _schedule_loop():
    # stable 64-bit key for singleton lock (any constant works)
    LOCK_KEY = 634269201837461123
    while True:
        try:
            with db_conn() as c:
                if not _try_advisory_lock(c, LOCK_KEY):
                    # Another worker holds the scheduler; sleep briefly and retry
                    time.sleep(15)
                    continue
            # We hold the lock: run sync immediately, then align to next boundary
            logger.info("[scheduler] tick → running sync()")
            asyncio.run(sync())
            sleep_s = _seconds_until_next_tick(datetime.now())
            logger.info(f"[scheduler] sleeping {sleep_s}s until next 6h boundary")
            time.sleep(sleep_s)
        except Exception as e:
            logger.exception("[scheduler] loop error: %s", e)
            time.sleep(30)

# Start scheduler (singleton via DB lock)
if ENABLE_SCHEDULER:
    threading.Thread(target=_schedule_loop, daemon=True).start()

# ────────────────────────────────────────────────────────────────────────────────
# Timezone helpers (single source of truth)
# ────────────────────────────────────────────────────────────────────────────────
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError  # py3.9+
except Exception:
    ZoneInfo = None
    class ZoneInfoNotFoundError(Exception):
        pass

DEFAULT_TZ = os.getenv("LOCAL_TZ") or "UTC"

def today_iso_local(tzkey: Optional[str] = None) -> str:
    tzname = (tzkey or DEFAULT_TZ or "UTC")
    if ZoneInfo:
        try:
            return datetime.now(ZoneInfo(tzname)).date().isoformat()
        except ZoneInfoNotFoundError:
            pass
        except Exception:
            pass
    return datetime.utcnow().date().isoformat()

# ────────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "3000"))
    logger.info(f"[startup] app booting on 0.0.0.0:{port} tz={LOCAL_TZ} output={OUTPUT_TARGET}")
    # Print route map to confirm /run_catalogue_health exists
    try:
        logger.info("[startup] routes: %s", [str(r) for r in app.url_map.iter_rules()])
    except Exception:
        pass
    app.run(host="0.0.0.0", port=port)
