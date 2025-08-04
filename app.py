# app.py
import os
import json
import csv
import time
import asyncio
import threading
from typing import Optional, Tuple, Dict, Any, List

import requests
from datetime import date
from flask import Flask, request, jsonify
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ==========
# HARDCODED CONFIGS
# ==========

# Airtable
AT_API_KEY = "pat98an7RiDkEBJjr.daa42ae34c7d54c8de334d45d09bb4e374e14091fb6dee48dc8659e1e855050c"
AT_BASE_ID = "appAmLhYAVcmKmRC3"

CATALOGUE_TABLE = "Catalogue"
CATALOGUE_VIEW = "Inner Catalogue"
CATALOGUE_ISRC_FIELD = "ISRC"

PLAYCOUNTS_TABLE = "SpotifyPlaycounts"
PLAYCOUNTS_LINK_FIELD = "ISRC"        # Linked record field to Catalogue
PLAYCOUNTS_DATE_FIELD = "Date"
PLAYCOUNTS_COUNT_FIELD = "Playcount"
PLAYCOUNTS_DELTA_FIELD = "Delta"

AIRTABLE_SLEEP_BETWEEN_WRITES = 0.2    # seconds between Airtable writes
SPOTIFY_SLEEP_BETWEEN_ALBUMS  = 0.15   # seconds between album calls

# Spotify (public API for ISRC search)
CLIENT_ID = "0fe3aee6708647338f3fec1673558a8c"
CLIENT_SECRET = "eb12c63ff8a24524af6c5aba83acfb9a"

# Spotify internal (captured op + hash + variables for v2)
OPERATION_NAME = "getAlbum"
PERSISTED_HASH = "97dd13a1f28c80d66115a13697a7ffd94fe3bebdb94da42159456e1d82bfee76"
CAPTURED_VARS = {"locale": "", "offset": 0, "limit": 50}

# HTTP headers / targets
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

# ==========
# AIRTABLE HELPERS
# ==========

def _at_headers_json() -> Dict[str, str]:
    return {"Authorization": f"Bearer {AT_API_KEY}", "Content-Type": "application/json"}

def _at_headers_bearer() -> Dict[str, str]:
    return {"Authorization": f"Bearer {AT_API_KEY}"}

def _at_base_url(table_name: str) -> str:
    return f"https://api.airtable.com/v0/{AT_BASE_ID}/{requests.utils.quote(table_name)}"

def airtable_list_catalogue_isrcs() -> List[Dict[str, Any]]:
    """
    Returns list of dicts: [{id: <Catalogue recId>, isrc: <ISRC>}]
    from Catalogue table filtered by the given view.
    """
    out: List[Dict[str, Any]] = []
    url = _at_base_url(CATALOGUE_TABLE)
    params = {
        "view": CATALOGUE_VIEW,
        "pageSize": 100,
        "fields[]": CATALOGUE_ISRC_FIELD,
    }
    offset = None
    while True:
        p = dict(params)
        if offset:
            p["offset"] = offset
        r = requests.get(url, headers=_at_headers_bearer(), params=p, timeout=30)
        r.raise_for_status()
        data = r.json()
        for rec in data.get("records", []):
            fields = rec.get("fields", {}) or {}
            isrc = (fields.get(CATALOGUE_ISRC_FIELD) or "").strip()
            if isrc:
                out.append({"id": rec["id"], "isrc": isrc})
        offset = data.get("offset")
        if not offset:
            break
    return out

def airtable_find_today_record(catalogue_rec_id: str, today_iso: str) -> Optional[str]:
    """
    Returns recordId of today's row for this linked Catalogue record, or None if not found.
    Uses IS_SAME({Date}, 'YYYY-MM-DD', 'day') to match the day exactly.
    """
    url = _at_base_url(PLAYCOUNTS_TABLE)
    formula = f"AND(SEARCH('{catalogue_rec_id}', ARRAYJOIN({{{{{ {PLAYCOUNTS_LINK_FIELD} }}}}})), IS_SAME({{{{{ {PLAYCOUNTS_DATE_FIELD} }}}}}, '{today_iso}', 'day'))"
    params = {"filterByFormula": formula, "pageSize": 1}
    r = requests.get(url, headers=_at_headers_bearer(), params=params, timeout=30)
    if r.status_code != 200:
        return None
    recs = r.json().get("records", [])
    return recs[0]["id"] if recs else None

def airtable_get_previous_playcount_before(catalogue_rec_id: str, before_iso: str) -> Optional[int]:
    """
    Returns the most recent playcount strictly before 'before_iso' (YYYY-MM-DD).
    """
    url = _at_base_url(PLAYCOUNTS_TABLE)
    formula = f"AND(SEARCH('{catalogue_rec_id}', ARRAYJOIN({{{{{ {PLAYCOUNTS_LINK_FIELD} }}}}})), {{{{{ {PLAYCOUNTS_DATE_FIELD} }}}}} < '{before_iso}')"
    params = {
        "filterByFormula": formula,
        "pageSize": 1,
        "sort[0][field]": PLAYCOUNTS_DATE_FIELD,
        "sort[0][direction]": "desc",
    }
    r = requests.get(url, headers=_at_headers_bearer(), params=params, timeout=30)
    if r.status_code != 200:
        return None
    recs = r.json().get("records", [])
    if not recs:
        return None
    val = (recs[0].get("fields") or {}).get(PLAYCOUNTS_COUNT_FIELD)
    try:
        return int(val)
    except Exception:
        return None

def airtable_upsert_playcount(catalogue_rec_id: str, date_iso: str, playcount: int) -> None:
    """
    Idempotent per-day write:
      - If today's record exists → PATCH it with new Playcount & recomputed Delta (vs latest before today).
      - Else → POST a new row with those fields.
    """
    url = _at_base_url(PLAYCOUNTS_TABLE)
    # Delta computed vs previous (strictly before today)
    prev_pc = airtable_get_previous_playcount_before(catalogue_rec_id, date_iso)
    delta_val = playcount - prev_pc if isinstance(prev_pc, int) else None

    existing_id = airtable_find_today_record(catalogue_rec_id, date_iso)
    fields = {
        PLAYCOUNTS_LINK_FIELD: [catalogue_rec_id],  # link stays present even on PATCH
        PLAYCOUNTS_DATE_FIELD: date_iso,
        PLAYCOUNTS_COUNT_FIELD: playcount,
    }
    if delta_val is not None:
        fields[PLAYCOUNTS_DELTA_FIELD] = delta_val

    if existing_id:
        # PATCH existing
        body = {"records": [{"id": existing_id, "fields": fields}], "typecast": False}
        resp = requests.patch(url, headers=_at_headers_json(), data=json.dumps(body), timeout=30)
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"Airtable UPDATE failed: {resp.status_code}: {resp.text[:300]}")
    else:
        # POST create
        body = {"records": [{"fields": fields}], "typecast": False}
        resp = requests.post(url, headers=_at_headers_json(), data=json.dumps(body), timeout=30)
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"Airtable INSERT failed: {resp.status_code}: {resp.text[:300]}")

    time.sleep(AIRTABLE_SLEEP_BETWEEN_WRITES)

# ==========
# SPOTIFY HELPERS
# ==========

def get_search_token() -> str:
    r = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        auth=(CLIENT_ID, CLIENT_SECRET),
        timeout=30,
        headers={"User-Agent": USER_AGENT},
    )
    r.raise_for_status()
    return r.json()["access_token"]

def search_track_by_isrc(isrc: str, bearer: str) -> Optional[Tuple[str, str, str]]:
    headers = {"Authorization": f"Bearer {bearer}", "User-Agent": USER_AGENT}
    params = {"q": f"isrc:{isrc}", "type": "track", "limit": 1}
    r = requests.get("https://api.spotify.com/v1/search", headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        return None
    items = (r.json().get("tracks") or {}).get("items") or []
    if not items:
        return None
    t = items[0]
    return t["id"], t["album"]["id"], t["name"]

def parse_playcount(js: Dict[str, Any], track_id: str) -> Optional[int]:
    # v2 schema
    try:
        items = js["data"]["albumUnion"]["tracksV2"]["items"]
        for it in items:
            tr = it.get("track") or {}
            if track_id in (tr.get("id"), (tr.get("uri") or "").split(":")[-1]):
                pc = tr.get("playcount")
                if pc is None:
                    return None
                if isinstance(pc, str):
                    s = pc.replace(",", "")
                    return int(s) if s.isdigit() else None
                if isinstance(pc, (int, float)):
                    return int(pc)
                return None
    except Exception:
        pass
    # fallback: older schema
    try:
        items = js["data"]["album"]["tracks"]["items"]
        for it in items:
            tr = it.get("track") or {}
            if tr.get("id") == track_id:
                pc = tr.get("playcount")
                if pc is None:
                    return None
                if isinstance(pc, str):
                    s = pc.replace(",", "")
                    return int(s) if s.isdigit() else None
                if isinstance(pc, (int, float)):
                    return int(pc)
                return None
    except Exception:
        pass
    return None

# ==========
# TOKEN SNIFF (Playwright page traffic)
# ==========

async def sniff_tokens_in_browser() -> Tuple[str, Optional[str]]:
    """
    Opens open.spotify.com headlessly and waits for a pathfinder v2 request,
    then captures Authorization (Bearer ...) and Client-Token headers.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = await browser.new_context(user_agent=USER_AGENT, locale="en-GB")
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = { runtime: {} };
            Object.defineProperty(navigator, 'languages', {get: () => ['en-GB','en']});
            Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
        """)
        page = await context.new_page()

        bearer: Optional[str] = None
        client_tok: Optional[str] = None
        got = asyncio.get_event_loop().create_future()

        async def on_response(resp):
            nonlocal bearer, client_tok
            u = resp.url or ""
            if "/pathfinder/v2/query" in u and resp.status == 200:
                try:
                    req = resp.request
                    h = req.headers or {}
                    auth = h.get("authorization") or h.get("Authorization")
                    if auth and auth.lower().startswith("bearer "):
                        bearer = auth.split(" ", 1)[1].strip()
                        client_tok = h.get("client-token") or h.get("Client-Token")
                        if not got.done():
                            got.set_result(True)
                except Exception:
                    pass

        page.on("response", on_response)

        try:
            await page.goto("https://open.spotify.com/", wait_until="domcontentloaded", timeout=35000)
        except PWTimeout:
            pass
        await page.wait_for_timeout(800)
        try:
            await page.goto("https://open.spotify.com/album/2noRn2Aes5aoNVsU6iWThc",
                            wait_until="domcontentloaded", timeout=35000)
            try:
                await asyncio.wait_for(got, timeout=12.0)
            except asyncio.TimeoutError:
                try:
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.wait_for(got, timeout=6.0)
                except asyncio.TimeoutError:
                    pass
        except Exception:
            pass

        await browser.close()
        if not bearer:
            raise RuntimeError("Did not observe Authorization header on any pathfinder v2 request.")
        return bearer, client_tok

# ==========
# CALL PATHFINDER V2
# ==========

def call_album_v2_exact(
    album_id: str,
    web_bearer: str,
    client_token: Optional[str],
    op_name: Optional[str],
    sha256_hash: str,
    captured_vars: Dict[str, Any],
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    s = session or requests.Session()
    headers = {
        "Authorization": f"Bearer {web_bearer}",
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://open.spotify.com/",
        "Origin": "https://open.spotify.com",
        "App-Platform": "Web Player",
        "content-type": "application/json",
    }
    if client_token:
        headers["Client-Token"] = client_token

    vars_copy = dict(captured_vars or {})
    vars_copy["uri"] = f"spotify:album:{album_id}"
    vars_copy.setdefault("offset", 0)
    vars_copy.setdefault("limit", 50)

    body = {
        "variables": vars_copy,
        "extensions": {"persistedQuery": {"version": 1, "sha256Hash": sha256_hash}},
    }
    if op_name:
        body["operationName"] = op_name

    errors = []
    for host in PATHFINDER_HOSTS:
        url = f"https://{host}/pathfinder/v2/query"
        try:
            r = s.post(url, headers=headers, json=body, timeout=30)
            if r.status_code == 200:
                return r.json()
            errors.append(f"{host}:{r.status_code}")
        except Exception as e:
            errors.append(f"{host}: {e}")
    raise RuntimeError("Pathfinder v2 failed: " + " | ".join(errors[:4]))

# ==========
# ORCHESTRATION
# ==========

async def run_sync():
    """
    One full run: read ISRCs, fetch counts, upsert Airtable (idempotent per day), write CSV for logs.
    """
    # 1) Catalogue → ISRCs
    cat_rows = airtable_list_catalogue_isrcs()
    if not cat_rows:
        print("No ISRCs found in the selected view.")
        return

    # Deduplicate by ISRC, keep first record id
    seen = set()
    isrc_order: List[str] = []
    cat_id_by_isrc: Dict[str, str] = {}
    for r in cat_rows:
        isrc = r["isrc"]
        if isrc not in seen:
            seen.add(isrc)
            isrc_order.append(isrc)
            cat_id_by_isrc[isrc] = r["id"]

    print(f"Running for {len(isrc_order)} unique ISRC(s).")

    # 2) Get tokens
    print("Sniffing Spotify tokens (Playwright headless)…")
    web_bearer, client_token = await sniff_tokens_in_browser()

    # 3) Spotify public token for search
    search_bearer = get_search_token()

    # 4) Resolve tracks/albums, then fetch playcounts per album
    s = requests.Session()
    track_album_by_isrc: Dict[str, Tuple[Optional[str], Optional[str], Optional[str]]] = {}
    album_cache: Dict[str, Dict[str, Any]] = {}

    # Resolve all ISRCs first
    for isrc in isrc_order:
        try:
            found = search_track_by_isrc(isrc, search_bearer)
            if found:
                t_id, a_id, t_name = found
                track_album_by_isrc[isrc] = (t_id, a_id, t_name)
            else:
                track_album_by_isrc[isrc] = (None, None, None)
        except Exception:
            track_album_by_isrc[isrc] = (None, None, None)

    # Call v2 once per unique album
    fetched_albums = set()
    for isrc, triple in track_album_by_isrc.items():
        t_id, a_id, _tname = triple
        if not a_id or a_id in fetched_albums:
            continue
        time.sleep(SPOTIFY_SLEEP_BETWEEN_ALBUMS)
        album_cache[a_id] = call_album_v2_exact(
            album_id=a_id,
            web_bearer=web_bearer,
            client_token=client_token,
            op_name=OPERATION_NAME,
            sha256_hash=PERSISTED_HASH,
            captured_vars=CAPTURED_VARS,
            session=s,
        )
        fetched_albums.add(a_id)

    # 5) Build CSV + Airtable UPSERTS (idempotent)
    today_iso = date.today().isoformat()
    csv_rows: List[Dict[str, Any]] = []

    for isrc in isrc_order:
        t_id, a_id, t_name = track_album_by_isrc.get(isrc, (None, None, None))
        row = {"isrc": isrc, "track_name": t_name, "play_count": None, "error": None}
        if not a_id or not t_id:
            row["error"] = "No album/track ID"
            csv_rows.append(row)
            continue

        js = album_cache.get(a_id)
        pc = parse_playcount(js, t_id)
        if pc is None:
            row["error"] = "Playcount missing"
            csv_rows.append(row)
            continue

        row["play_count"] = pc
        csv_rows.append(row)

        # UPSERT today's row (overwrites if already exists)
        cat_rec_id = cat_id_by_isrc[isrc]
        airtable_upsert_playcount(cat_rec_id, today_iso, pc)

    # 6) Write CSV (audit/log convenience)
    with open("playcounts.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["isrc", "track_name", "play_count", "error"])
        w.writeheader()
        w.writerows(csv_rows)
    print(f"Done. Wrote {len(csv_rows)} rows to playcounts.csv")

    # Log a short summary to stdout
    for r in csv_rows:
        print(r)

# ==========
# FLASK APP (Webhook to trigger run)
# ==========

app = Flask(__name__)

@app.get("/health")
def health():
    return {"ok": True}, 200

@app.post("/run")
def trigger_run():
    """
    POST /run
      - query ?async=1 to return immediately and run in background
      - otherwise blocks until the job completes
    """
    run_async = request.args.get("async", "0") in ("1", "true", "yes")

    def worker():
        asyncio.run(run_sync())

    if run_async:
        threading.Thread(target=worker, daemon=True).start()
        return jsonify({"status": "started"}), 202
    else:
        asyncio.run(run_sync())
        return jsonify({"status": "ok"}), 200

# ==========
# ENTRYPOINT
# ==========

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "3000"))
    app.run(host="0.0.0.0", port=port)
