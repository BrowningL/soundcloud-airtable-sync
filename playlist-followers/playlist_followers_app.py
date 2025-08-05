import time
import json
import requests
from datetime import date
from typing import Dict, Any, List, Optional, Tuple
from flask import Flask, request, jsonify

# ==========
# HARDCODED CONFIG (your tables & keys)
# ==========

# Airtable
AT_API_KEY = "pat98an7RiDkEBJjr.daa42ae34c7d54c8de334d45d09bb4e374e14091fb6dee48dc8659e1e855050c"
AT_BASE_ID = "appAmLhYAVcmKmRC3"

# Tables / fields
PLAYLISTS_TABLE = "Playlists"
PLAYLISTS_VIEW = None                 # put a view name if you want to limit, else leave None
PLAYLISTS_ID_FIELD = "Playlist ID"    # can be raw ID / URI / URL

FOLLOWERS_TABLE = "Playlist Followers"
FOLLOWERS_DATE_FIELD = "Date"
FOLLOWERS_COUNT_FIELD = "Followers"
FOLLOWERS_LINK_FIELD = "Playlist"     # linked-record to Playlists

# Spotify (client credentials OK for public playlists)
SPOTIFY_CLIENT_ID = "0fe3aee6708647338f3fec1673558a8c"
SPOTIFY_CLIENT_SECRET = "eb12c63ff8a24524af6c5aba83acfb9a"

# QoL / safety
AIRTABLE_SLEEP_BETWEEN_WRITES = 0.2
SPOTIFY_SLEEP_BETWEEN_CALLS = 0.15
MAX_RETRIES = 3

# Optional protection for your /run endpoint
RUN_TOKEN = "set-a-secret-token-here"   # change this; include it as ?token=... when calling /run

# ---------- helpers

def _at_headers_json() -> Dict[str, str]:
    return {"Authorization": f"Bearer {AT_API_KEY}", "Content-Type": "application/json"}

def _at_headers_bearer() -> Dict[str, str]:
    return {"Authorization": f"Bearer {AT_API_KEY}"}

def _at_url(table: str) -> str:
    from urllib.parse import quote
    return f"https://api.airtable.com/v0/{AT_BASE_ID}/{quote(table)}"

def normalize_playlist_id(val: str) -> Optional[str]:
    """Accepts raw ID, spotify:playlist:ID, or https://open.spotify.com/playlist/ID?..."""
    if not val:
        return None
    v = val.strip()
    if v.startswith("spotify:playlist:"):
        return v.split(":")[-1]
    if "open.spotify.com/playlist/" in v:
        part = v.split("open.spotify.com/playlist/")[-1]
        part = part.split("?")[0].split("#")[0]
        return part
    v = v.split("?")[0].split("#")[0]
    return v

# ---------- Airtable

def airtable_list_playlists() -> List[Dict[str, str]]:
    """Return [{id: <Playlists recId>, playlist_id: <normalized spotify ID>}]."""
    out: List[Dict[str, str]] = []
    url = _at_url(PLAYLISTS_TABLE)
    params = {"pageSize": 100, "fields[]": PLAYLISTS_ID_FIELD}
    if PLAYLISTS_VIEW:
        params["view"] = PLAYLISTS_VIEW
    offset = None
    while True:
        p = dict(params)
        if offset:
            p["offset"] = offset
        r = requests.get(url, headers=_at_headers_bearer(), params=p, timeout=30)
        r.raise_for_status()
        data = r.json()
        for rec in data.get("records", []):
            raw = (rec.get("fields", {}) or {}).get(PLAYLISTS_ID_FIELD, "")
            pid = normalize_playlist_id(raw)
            if pid:
                out.append({"id": rec["id"], "playlist_id": pid})
        offset = data.get("offset")
        if not offset:
            break
    return out

def airtable_find_today_row(playlist_rec_id: str, today_iso: str) -> Optional[str]:
    """Return recordId in Playlist Followers for this playlist on today’s date, else None."""
    url = _at_url(FOLLOWERS_TABLE)
    # IS_SAME({Date}, 'YYYY-MM-DD', 'day') so you can re-run safely in the same day
    formula = (
        f"AND(SEARCH('{playlist_rec_id}', ARRAYJOIN({{{{{ {FOLLOWERS_LINK_FIELD} }}}}})), "
        f"IS_SAME({{{{{ {FOLLOWERS_DATE_FIELD} }}}}}, '{today_iso}', 'day'))"
    )
    r = requests.get(url, headers=_at_headers_bearer(), params={"filterByFormula": formula, "pageSize": 1}, timeout=30)
    if r.status_code != 200:
        return None
    recs = r.json().get("records", [])
    return recs[0]["id"] if recs else None

def airtable_upsert_followers(playlist_rec_id: str, date_iso: str, followers: int) -> None:
    """Idempotent upsert for (Playlist, Date) -> Followers."""
    url = _at_url(FOLLOWERS_TABLE)
    existing_id = airtable_find_today_row(playlist_rec_id, date_iso)

    fields = {
        FOLLOWERS_DATE_FIELD: date_iso,
        FOLLOWERS_COUNT_FIELD: followers,
        FOLLOWERS_LINK_FIELD: [playlist_rec_id],
    }

    if existing_id:
        payload = {"records": [{"id": existing_id, "fields": fields}], "typecast": False}
        resp = requests.patch(url, headers=_at_headers_json(), data=json.dumps(payload), timeout=30)
    else:
        payload = {"records": [{"fields": fields}], "typecast": False}
        resp = requests.post(url, headers=_at_headers_json(), data=json.dumps(payload), timeout=30)

    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Airtable upsert failed: {resp.status_code}: {resp.text[:300]}")
    time.sleep(AIRTABLE_SLEEP_BETWEEN_WRITES)

# ---------- Spotify Web API

def get_spotify_app_token() -> str:
    r = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        auth=(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET),
        timeout=25,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def get_playlist_followers(playlist_id: str, bearer: str) -> Optional[int]:
    """Return followers.total for a public playlist; None if not accessible."""
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}"
    params = {"fields": "followers.total"}  # only what we need
    headers = {"Authorization": f"Bearer {bearer}"}

    for attempt in range(1, MAX_RETRIES + 1):
        r = requests.get(url, headers=headers, params=params, timeout=20)
        if r.status_code == 200:
            try:
                return int(r.json().get("followers", {}).get("total", 0))
            except Exception:
                return None
        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", "1"))
            time.sleep(retry_after + 1)
            continue
        # 401/403/404 → likely private/unavailable or invalid ID; log + stop retrying
        if r.status_code in (401, 403, 404):
            try:
                body = r.json()
            except Exception:
                body = r.text
            print(f"[debug] {playlist_id}: {r.status_code} -> {body}")
            return None
        # transient? backoff a bit
        time.sleep(0.5 * attempt)
    return None

# ---------- Sync

def sync_once() -> Tuple[int, int]:
    playlists = airtable_list_playlists()
    if not playlists:
        print("No playlists found.")
        return (0, 0)

    # De-dupe by playlist_id (keep first record link)
    seen = set()
    order: List[Tuple[str, str]] = []  # (playlist_id, playlist_rec_id)
    for r in playlists:
        pid = r["playlist_id"]
        if pid not in seen:
            seen.add(pid)
            order.append((pid, r["id"]))

    print(f"Found {len(order)} playlists in Airtable.")
    bearer = get_spotify_app_token()
    today_iso = date.today().isoformat()

    ok, failed = 0, 0
    for pid, rec_id in order:
        time.sleep(SPOTIFY_SLEEP_BETWEEN_CALLS)
        count = get_playlist_followers(pid, bearer)
        if count is None:
            print(f"[skip] {pid}: could not fetch followers (private/unavailable)")
            failed += 1
            continue
        try:
            airtable_upsert_followers(rec_id, today_iso, count)
            ok += 1
        except Exception as e:
            print(f"[airtable error] {pid}: {e}")
            failed += 1

    print(f"Done. Upserted OK={ok}, failed={failed}.")
    return ok, failed

# ---------- Flask app

app = Flask(__name__)

@app.get("/health")
def health():
    return {"ok": True}, 200

@app.post("/run")
def run_now():
    # simple token gate so randos can’t hammer your endpoint
    token = request.args.get("token")
    if RUN_TOKEN and token != RUN_TOKEN:
        return jsonify({"error": "unauthorized"}), 401

    ok, failed = sync_once()
    return jsonify({"ok": ok, "failed": failed}), 200

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", "3000"))
    # optional: set RUN_TOKEN via env instead of hardcoding
    app.run(host="0.0.0.0", port=port)

