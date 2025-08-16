import os
import time
import asyncio
import threading
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, request
from playwright.async_api import async_playwright

# Postgres
import psycopg2
from psycopg2.extras import RealDictCursor

# ────────────────────────────────────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────────────────────────────────────
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY") or os.getenv("AT_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID") or os.getenv("AT_BASE_ID") or "appAmLhYAVcmKmRC3"
if not AIRTABLE_API_KEY or not AIRTABLE_API_KEY.startswith("pat"):
    raise RuntimeError("Set AIRTABLE_API_KEY to a valid Airtable Personal Access Token (starts with 'pat').")

DATABASE_URL = os.getenv("DATABASE_URL")  # e.g. postgresql://railway:***@trolley.proxy.rlwy.net:33798/timeseriesdb
OUTPUT_TARGET = os.getenv("OUTPUT_TARGET", "postgres").lower()  # 'postgres' | 'airtable' | 'both'

# ----- Catalogue (ISRC list) -----
CATALOGUE_TABLE = os.getenv("CATALOGUE_TABLE", "Catalogue")
CATALOGUE_VIEW = os.getenv("CATALOGUE_VIEW", "Inner Catalogue")
CATALOGUE_ISRC_FIELD = os.getenv("CATALOGUE_ISRC_FIELD", "ISRC")

# ----- Track Playcounts (Airtable) -----
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

# Spotify web GraphQL
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

def db_upsert_playlist_followers(cur, platform: str, playlist_id: str, day_iso: str, followers: int, playlist_name: str = None):
    cur.execute("""
        INSERT INTO playlist_followers(platform, playlist_id, snapshot_date, followers, playlist_name)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (platform, playlist_id, snapshot_date)
        DO UPDATE SET followers = EXCLUDED.followers,
                      playlist_name = COALESCE(EXCLUDED.playlist_name, playlist_followers.playlist_name)
    """, (platform, playlist_id, day_iso, followers, playlist_name))

# ────────────────────────────────────────────────────────────────────────────────
# Catalogue / ISRC list
# ────────────────────────────────────────────────────────────────────────────────
def list_isrcs() -> List[Dict[str, str]]:
    rows, offset = [], None
    while True:
        params = {"view": CATALOGUE_VIEW, "pageSize": 100, "fields[]": CATALOGUE_ISRC_FIELD}
        if offset:
            params["offset"] = offset
        r = requests.get(at_url(CATALOGUE_TABLE), headers=at_headers(), params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        for rec in data["records"]:
            isrc = rec.get("fields", {}).get(CATALOGUE_ISRC_FIELD)
            if isrc:
                rows.append({"id": rec["id"], "isrc": isrc})
        offset = data.get("offset")
        if not offset:
            break
    return rows

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
            f"SEARCH('{_q(isrc_code)}', ARRAYJOIN({{{PLAYCOUNTS_LINK_FIELD}}})),"  # linked → ARRAY
            f"IS_SAME({{{PLAYCOUNTS_DATE_FIELD}}}, '{day_iso}', 'day')"
            f")"
        )
    r = requests.get(at_url(PLAYCOUNTS_TABLE), headers=at_headers(), params={"filterByFormula": formula, "pageSize": 1}, timeout=60)
    if r.status_code != 200: return None
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
# Playlists master (Airtable → list of IDs to track)
# ────────────────────────────────────────────────────────────────────────────────
def playlists_index_from_airtable():
    rows = at_paginate(PLAYLISTS_TABLE, {
        "pageSize": 100,
        "fields[]": [PLAYLISTS_NAME_FIELD, PLAYLISTS_ID_FIELD]
    })
    out = {}
    for r in rows:
        f = r.get("fields", {})
        pid = f.get(PLAYLISTS_ID_FIELD)
        name = f.get(PLAYLISTS_NAME_FIELD)
        if pid:
            out[r["id"]] = {"playlist_id": pid, "name": name}
    return out

# ────────────────────────────────────────────────────────────────────────────────
# Spotify helpers (unchanged)
# ────────────────────────────────────────────────────────────────────────────────
def get_search_token() -> str:
    r = requests.post("https://accounts.spotify.com/api/token", data={"grant_type": "client_credentials"}, auth=(CLIENT_ID, CLIENT_SECRET), timeout=60)
    r.raise_for_status()
    return r.json()["access_token"]

def search_track(isrc: str, bearer: str) -> Optional[Tuple[str, str, str]]:
    r = requests.get("https://api.spotify.com/v1/search", headers={"Authorization": f"Bearer {bearer}"},
                     params={"q": f"isrc:{isrc}", "type": "track", "limit": 5}, timeout=60)
    if r.status_code != 200:
        return None
    for t in r.json().get("tracks", {}).get("items", []):
        if t.get("external_ids", {}).get("isrc", "").upper() == isrc.upper():
            return t["id"], t["album"]["id"], t["name"]
    items = r.json().get("tracks", {}).get("items", [])
    if items:
        return items[0]["id"], items[0]["album"]["id"], items[0]["name"]
    return None

async def sniff_tokens() -> Tuple[str, Optional[str]]:
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
# Main sync for Spotify streams (writes to Postgres and/or Airtable)
# ────────────────────────────────────────────────────────────────────────────────
async def sync():
    rows = list_isrcs()
    seen, order, linkmap = set(), [], {}
    for rec in rows:
        code = rec["isrc"]
        if code not in seen:
            seen.add(code)
            order.append(code)
            linkmap[code] = rec["id"]

    print(f"Syncing {len(order)} tracks...")
    web_tok, cli_tok = await sniff_tokens()
    search_tok = get_search_token()
    cache: Dict[str, Dict[str, Any]] = {}
    today_iso = date.today().isoformat()

    conn = None
    cur = None
    if OUTPUT_TARGET in ("postgres", "both"):
        conn = db_conn()
        cur = conn.cursor()
        db_ensure_platform(cur, "spotify")

    try:
        for isrc in order:
            tr = search_track(isrc, search_tok)
            if not tr:
                if OUTPUT_TARGET in ("airtable", "both"):
                    upsert_count(linkmap[isrc], isrc, today_iso, 0)
                if OUTPUT_TARGET in ("postgres", "both"):
                    track_uid = db_upsert_track(cur, isrc, None, None)
                    db_upsert_stream(cur, "spotify", track_uid, today_iso, 0)
                continue

            tid, aid, _ = tr
            if aid not in cache:
                time.sleep(spotify_sleep)
                cache[aid] = fetch_album(aid, web_tok, cli_tok)

            pc = 0
            js = cache[aid]
            for it in js.get("data", {}).get("albumUnion", {}).get("tracksV2", {}).get("items", []):
                track = it.get("track", {})
                cand = (track.get("uri", "") or "").split(":")[-1] if track.get("uri") else None
                if tid in (track.get("id"), cand):
                    val = track.get("playcount") or 0
                    pc = int(str(val).replace(",", ""))
                    break

            if OUTPUT_TARGET in ("airtable", "both"):
                upsert_count(linkmap[isrc], isrc, today_iso, pc)
            if OUTPUT_TARGET in ("postgres", "both"):
                track_uid = db_upsert_track(cur, isrc, None, None)
                db_upsert_stream(cur, "spotify", track_uid, today_iso, pc)

        if conn:
            conn.commit()
    finally:
        if cur: cur.close()
        if conn: conn.close()

    print("Completed sync.")

# ────────────────────────────────────────────────────────────────────────────────
# Backfills (Airtable → Postgres)
# ────────────────────────────────────────────────────────────────────────────────
def backfill_airtable_to_postgres(days: Optional[str] = None) -> int:
    """
    days:
      - 'all' (or None/0/negative) => backfill ALL rows
      - positive integer string => only last N days
    """
    recs = at_paginate(CATALOGUE_TABLE, {"view": CATALOGUE_VIEW, "pageSize": 100, "fields[]": CATALOGUE_ISRC_FIELD})
    cat_id_to_isrc = {r["id"]: r["fields"].get(CATALOGUE_ISRC_FIELD) for r in recs if r.get("fields", {}).get(CATALOGUE_ISRC_FIELD)}

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
                isrc = cat_id_to_isrc.get(links[0])
                if not isrc:
                    continue
                day_iso = f.get(PLAYCOUNTS_DATE_FIELD)
                try:
                    count = int(f.get(PLAYCOUNTS_COUNT_FIELD, 0) or 0)
                except Exception:
                    count = 0
                track_uid = db_upsert_track(cur, isrc, None, None)
                db_upsert_stream(cur, "spotify", track_uid, day_iso, count)
                inserted += 1
        conn.commit()
    return inserted

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
            playlist_id = meta["playlist_id"]
            playlist_name = meta.get("name")
            day_iso = f.get(FOLLOWERS_DATE_FIELD)
            try:
                followers = int(f.get(FOLLOWERS_COUNT_FIELD, 0) or 0)
            except Exception:
                followers = 0
            db_upsert_playlist_followers(cur, platform, playlist_id, day_iso, followers, playlist_name)
            inserted += 1
        conn.commit()
    return inserted

# ────────────────────────────────────────────────────────────────────────────────
# Flask
# ────────────────────────────────────────────────────────────────────────────────
app = Flask(__name__)

@app.get("/health")
def health():
    return jsonify({"ok": True, "base": AIRTABLE_BASE_ID, "output_target": OUTPUT_TARGET})

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

@app.post("/run")
def run_endpoint():
    async_flag = request.args.get("async", "0").lower() in ("1", "true", "yes")
    if async_flag:
        threading.Thread(target=lambda: asyncio.run(sync()), daemon=True).start()
        return jsonify({"status": "started"}), 202
    else:
        asyncio.run(sync())
        return jsonify({"status": "completed"}), 200

@app.post("/backfill")
def backfill_endpoint():
    changed = backfill_deltas_for_all_tracks()
    return jsonify({"status": "backfilled_playcounts", "changed": changed}), 200

@app.post("/backfill_followers")
def backfill_followers_endpoint():
    changed = backfill_deltas_for_followers()
    return jsonify({"status": "backfilled_followers", "changed": changed}), 200

@app.post("/backfill_to_db")
def backfill_to_db_endpoint():
    days = request.args.get("days", "all")
    n = backfill_airtable_to_postgres(days=days)
    return jsonify({"status": "ok", "inserted_or_updated": n, "window_days": days}), 200

@app.post("/backfill_followers_to_db")
def backfill_followers_to_db():
    n = backfill_playlist_followers_all(platform="spotify")
    return jsonify({"ok": True, "inserted_or_updated": n}), 200

# Optional push-mode ingest for followers
# Body: { "platform":"spotify", "records":[{"playlist_id":"5KV...","date":"YYYY-MM-DD","followers":1234,"playlist_name":"optional"}] }
@app.post("/ingest/playlist_followers")
def ingest_playlist_followers():
    data = request.get_json(force=True) or {}
    platform = (data.get("platform") or "spotify").lower()
    records = data.get("records") or []
    if not records:
        return jsonify({"ok": False, "error": "no records"}), 400
    with db_conn() as conn, conn.cursor() as cur:
        db_ensure_platform(cur, platform)
        for rec in records:
            pid = rec.get("playlist_id")
            day = rec.get("date")
            followers = int(rec.get("followers") or 0)
            name = rec.get("playlist_name")
            if not (pid and day):
                continue
            db_upsert_playlist_followers(cur, platform, pid, day, followers, name)
        conn.commit()
    return jsonify({"ok": True, "count": len(records)}), 200

@app.get("/diag")
def diag():
    isrc = request.args.get("isrc")
    day  = request.args.get("day")
    if not isrc or not day:
        return jsonify({"error": "isrc & day required"}), 400
    return jsonify({"find_today_id": find_today_by_isrc(isrc, day), "prev_count": prev_count_by_isrc(isrc, day)})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "3000"))
    app.run(host="0.0.0.0", port=port)
