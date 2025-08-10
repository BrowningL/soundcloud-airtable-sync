import os
import json
import time
import asyncio
import threading
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, request
from playwright.async_api import async_playwright

# ────────────────────────────────────────────────────────────────────────────────
# CONFIG (use env vars in prod)
# ────────────────────────────────────────────────────────────────────────────────
AT_API_KEY = os.getenv("AT_API_KEY", "YOUR_AT_API_KEY")
AT_BASE_ID = os.getenv("AT_BASE_ID", "appAmLhYAVcmKmRC3")

CATALOGUE_TABLE = os.getenv("CATALOGUE_TABLE", "Catalogue")
CATALOGUE_VIEW = os.getenv("CATALOGUE_VIEW", "Inner Catalogue")
CATALOGUE_ISRC_FIELD = os.getenv("CATALOGUE_ISRC_FIELD", "ISRC")  # plain text ISRC field in Catalogue

PLAYCOUNTS_TABLE = os.getenv("PLAYCOUNTS_TABLE", "SpotifyPlaycounts")
PLAYCOUNTS_LINK_FIELD = os.getenv("PLAYCOUNTS_LINK_FIELD", "ISRC")  # linked → Catalogue
PLAYCOUNTS_DATE_FIELD = os.getenv("PLAYCOUNTS_DATE_FIELD", "Date")
PLAYCOUNTS_COUNT_FIELD = os.getenv("PLAYCOUNTS_COUNT_FIELD", "Playcount")
PLAYCOUNTS_DELTA_FIELD = os.getenv("PLAYCOUNTS_DELTA_FIELD", "Delta")

# Optional: if you add a text field "Key" to SpotifyPlaycounts, set this env var to its name
# When set, we enforce idempotency using Key = "{ISRC}|{YYYY-MM-DD}" (recommended but optional)
PLAYCOUNTS_KEY_FIELD = os.getenv("PLAYCOUNTS_KEY_FIELD")  # e.g., "Key"

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "YOUR_SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "YOUR_SPOTIFY_CLIENT_SECRET")

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
    return {"Authorization": f"Bearer {AT_API_KEY}", "Content-Type": "application/json"}

def at_url(table: str) -> str:
    return f"https://api.airtable.com/v0/{AT_BASE_ID}/{requests.utils.quote(table)}"

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
# Find today's row / previous count (by ISRC text)
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

def prev_count_by_isrc(isrc_code: str, before_iso: str, ignore_zero: bool = True) -> Optional[int]:
    clauses = [
        f"SEARCH('{_q(isrc_code)}', ARRAYJOIN({{{PLAYCOUNTS_LINK_FIELD}}}))",
        f"{{{PLAYCOUNTS_DATE_FIELD}}} < '{before_iso}'"
    ]
    if ignore_zero:
        clauses.append(f"{{{PLAYCOUNTS_COUNT_FIELD}}} > 0")
    formula = "AND(" + ",".join(clauses) + ")"
    params = {
        "filterByFormula": formula,
        "pageSize": 1,
        "sort[0][field]": PLAYCOUNTS_DATE_FIELD,
        "sort[0][direction]": "desc",
    }
    r = requests.get(at_url(PLAYCOUNTS_TABLE), headers=at_headers(), params=params, timeout=60)
    if r.status_code != 200:
        return None
    recs = r.json().get("records", [])
    if not recs:
        return None
    try:
        return int(recs[0]["fields"].get(PLAYCOUNTS_COUNT_FIELD, 0) or 0)
    except Exception:
        return None

# ────────────────────────────────────────────────────────────────────────────────
# Upsert playcount (+ delta)
# ────────────────────────────────────────────────────────────────────────────────
def upsert_count(linked_catalogue_rec_id: str, isrc_code: str, day_iso: str, count: int):
    existing_id = find_today_by_isrc(isrc_code, day_iso)
    prev = prev_count_by_isrc(isrc_code, day_iso)

    # Delta rule: only positive increases from positive baselines; else 0
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
# Spotify
# ────────────────────────────────────────────────────────────────────────────────
def get_search_token() -> str:
    r = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        auth=(CLIENT_ID, CLIENT_SECRET),
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def search_track(isrc: str, bearer: str) -> Optional[Tuple[str, str, str]]:
    r = requests.get(
        "https://api.spotify.com/v1/search",
        headers={"Authorization": f"Bearer {bearer}"},
        params={"q": f"isrc:{isrc}", "type": "track", "limit": 5},
        timeout=60,
    )
    if r.status_code != 200:
        return None
    for t in r.json().get("tracks", {}).get("items", []):
        if t.get("external_ids", {}).get("isrc", "").upper() == isrc.upper():
            return t["id"], t["album"]["id"], t["name"]
    items = r.json().get("tracks", {}).get("items", [])
    if items:
        return items[0]["id"], items[0]["album"]["id"], items[0]["name"]
    return None

# ────────────────────────────────────────────────────────────────────────────────
# Sniff Spotify web token for playcount GraphQL
# ────────────────────────────────────────────────────────────────────────────────
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

# ────────────────────────────────────────────────────────────────────────────────
# Fetch album via GraphQL v2
# ────────────────────────────────────────────────────────────────────────────────
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
# Backfill (recompute all deltas group-wise)
# ────────────────────────────────────────────────────────────────────────────────
def _recompute_deltas_for_groups(groups: Dict[str, List[Dict[str, Any]]]) -> int:
    updates: List[Dict[str, Any]] = []
    changed = 0
    for _, items in groups.items():
        items.sort(key=lambda r: r["fields"].get(PLAYCOUNTS_DATE_FIELD, "") or "")
        prev_val: Optional[int] = None
        for rec in items:
            f = rec.get("fields", {})
            cur = int(f.get(PLAYCOUNTS_COUNT_FIELD, 0) or 0)
            if prev_val is None:
                new_delta = None
            else:
                raw = cur - prev_val
                new_delta = raw if (cur > 0 and prev_val > 0 and raw > 0) else 0
            if new_delta is not None and f.get(PLAYCOUNTS_DELTA_FIELD) != new_delta:
                updates.append({"id": rec["id"], "fields": {PLAYCOUNTS_DELTA_FIELD: new_delta}})
                changed += 1
            prev_val = cur
            if len(updates) == 10:
                at_batch_patch(PLAYCOUNTS_TABLE, updates)
                updates.clear()
                time.sleep(airtable_sleep)
    if updates:
        at_batch_patch(PLAYCOUNTS_TABLE, updates)
    return changed

def backfill_deltas_for_all() -> int:
    recs = at_paginate(PLAYCOUNTS_TABLE, {"pageSize": 100, "sort[0][field]": PLAYCOUNTS_DATE_FIELD, "sort[0][direction]": "asc"})
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for rec in recs:
        links = rec.get("fields", {}).get(PLAYCOUNTS_LINK_FIELD, [])
        if not links:
            continue
        key = links[0]  # linked Catalogue recordId
        groups.setdefault(key, []).append(rec)
    return _recompute_deltas_for_groups(groups)

# ────────────────────────────────────────────────────────────────────────────────
# Main sync
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

    for isrc in order:
        tr = search_track(isrc, search_tok)
        if not tr:
            upsert_count(linkmap[isrc], isrc, today_iso, 0)
            continue
        tid, aid, _ = tr
        if aid not in cache:
            time.sleep(spotify_sleep)
            cache[aid] = fetch_album(aid, web_tok, cli_tok)

        pc = 0
        js = cache[aid]
        for it in js.get("data", {}).get("albumUnion", {}).get("tracksV2", {}).get("items", []):
            track = it.get("track", {})
            if tid in (track.get("id"), (track.get("uri", "").split(":")[-1] if track.get("uri") else None)):
                val = track.get("playcount") or 0
                pc = int(str(val).replace(",", ""))
                break

        upsert_count(linkmap[isrc], isrc, today_iso, pc)

    print("Completed sync.")

# ────────────────────────────────────────────────────────────────────────────────
# Flask
# ────────────────────────────────────────────────────────────────────────────────
app = Flask(__name__)

@app.get("/health")
def health():
    return jsonify({"ok": True})

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
    changed = backfill_deltas_for_all()
    return jsonify({"status": "backfilled", "changed": changed}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "3000"))
    app.run(host="0.0.0.0", port=port)
