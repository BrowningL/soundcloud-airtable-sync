# app_hardcoded.py
import os
import json
import csv
import asyncio
import time
from typing import Optional, Tuple, Dict, Any, List

import requests
from datetime import datetime, timezone, date
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ==========
# HARDCODED CONFIGS (replace as needed)
# ==========

AT_API_KEY = "pat98an7RiDkEBJjr.daa42ae34c7d54c8de334d45d09bb4e374e14091fb6dee48dc8659e1e855050c"
AT_BASE_ID = "appAmLhYAVcmKmRC3"

CATALOGUE_TABLE = "Catalogue"
CATALOGUE_VIEW = "Inner Catalogue"
CATALOGUE_ISRC_FIELD = "ISRC"

PLAYCOUNTS_TABLE = "SpotifyPlaycounts"
PLAYCOUNTS_LINK_FIELD = "ISRC"
PLAYCOUNTS_DATE_FIELD = "Date"
PLAYCOUNTS_COUNT_FIELD = "Playcount"
PLAYCOUNTS_DELTA_FIELD = "Delta"

AIRTABLE_SLEEP_BETWEEN_WRITES = 0.2
SPOTIFY_SLEEP_BETWEEN_ALBUMS = 0.15

CLIENT_ID = "0fe3aee6708647338f3fec1673558a8c"
CLIENT_SECRET = "eb12c63ff8a24524af6c5aba83acfb9a"

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

# ==== Airtable helpers ====
def _at_headers_json():
    return {"Authorization": f"Bearer {AT_API_KEY}", "Content-Type": "application/json"}

def _at_base_url(table_name):
    return f"https://api.airtable.com/v0/{AT_BASE_ID}/{requests.utils.quote(table_name)}"

def airtable_list_catalogue_isrcs():
    out = []
    url = _at_base_url(CATALOGUE_TABLE)
    params = {"view": CATALOGUE_VIEW, "pageSize": 100, "fields[]": CATALOGUE_ISRC_FIELD}
    offset = None
    while True:
        if offset: params["offset"] = offset
        r = requests.get(url, headers=_at_headers_json(), params=params)
        r.raise_for_status()
        data = r.json()
        for rec in data.get("records", []):
            isrc = rec["fields"].get(CATALOGUE_ISRC_FIELD, "").strip()
            if isrc:
                out.append({"id": rec["id"], "isrc": isrc})
        offset = data.get("offset")
        if not offset: break
    return out

def airtable_get_previous_playcount(catalogue_rec_id):
    url = _at_base_url(PLAYCOUNTS_TABLE)
    formula = f"SEARCH('{catalogue_rec_id}', ARRAYJOIN({{{PLAYCOUNTS_LINK_FIELD}}}))"
    params = {
        "filterByFormula": formula,
        "pageSize": 1,
        "sort[0][field]": PLAYCOUNTS_DATE_FIELD,
        "sort[0][direction]": "desc",
    }
    r = requests.get(url, headers=_at_headers_json(), params=params)
    if r.status_code != 200: return None
    records = r.json().get("records", [])
    if not records: return None
    val = records[0].get("fields", {}).get(PLAYCOUNTS_COUNT_FIELD)
    try: return int(val)
    except: return None

def airtable_batch_insert_playcounts(rows):
    if not rows: return
    url = _at_base_url(PLAYCOUNTS_TABLE)
    def to_record(r):
        fields = {
            PLAYCOUNTS_LINK_FIELD: [r["catalogue_rec_id"]],
            PLAYCOUNTS_DATE_FIELD: r["date_iso"],
            PLAYCOUNTS_COUNT_FIELD: r["playcount"],
        }
        if r.get("delta") is not None:
            fields[PLAYCOUNTS_DELTA_FIELD] = r["delta"]
        return {"fields": fields}
    for i in range(0, len(rows), 10):
        chunk = rows[i:i+10]
        body = {"records": [to_record(x) for x in chunk]}
        resp = requests.post(url, headers=_at_headers_json(), data=json.dumps(body))
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"Airtable insert failed: {resp.status_code}: {resp.text}")
        time.sleep(AIRTABLE_SLEEP_BETWEEN_WRITES)

# ==== Spotify helpers ====
def get_search_token():
    r = requests.post("https://accounts.spotify.com/api/token",
                     data={"grant_type": "client_credentials"},
                     auth=(CLIENT_ID, CLIENT_SECRET))
    r.raise_for_status()
    return r.json()["access_token"]

def search_track_by_isrc(isrc, bearer):
    headers = {"Authorization": f"Bearer {bearer}", "User-Agent": USER_AGENT}
    r = requests.get("https://api.spotify.com/v1/search",
                    headers=headers,
                    params={"q": f"isrc:{isrc}", "type": "track", "limit": 1})
    r.raise_for_status()
    items = r.json().get("tracks", {}).get("items", [])
    if not items: return None
    t = items[0]
    return t["id"], t["album"]["id"], t["name"]

def parse_playcount(js, track_id):
    try:
        for it in js["data"]["albumUnion"]["tracksV2"]["items"]:
            tr = it.get("track") or {}
            if track_id in (tr.get("id"), tr.get("uri", "").split(":")[-1]):
                pc = tr.get("playcount")
                return int(pc.replace(",", "")) if isinstance(pc, str) else int(pc)
    except: return None

# ==== Sniff tokens ====
async def sniff_tokens_in_browser():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(user_agent=USER_AGENT)
        page = await context.new_page()
        token_future = asyncio.get_event_loop().create_future()
        async def on_response(resp):
            if "/pathfinder/v2/query" in resp.url:
                h = resp.request.headers
                bearer = h.get("authorization") or h.get("Authorization")
                if bearer and bearer.lower().startswith("bearer"):
                    token = bearer.split(" ", 1)[1]
                    client = h.get("client-token") or h.get("Client-Token")
                    if not token_future.done():
                        token_future.set_result((token, client))
        page.on("response", on_response)
        await page.goto("https://open.spotify.com/album/2noRn2Aes5aoNVsU6iWThc")
        try:
            return await asyncio.wait_for(token_future, timeout=15)
        except:
            raise RuntimeError("Token sniff failed")
        finally:
            await browser.close()

# ==== Album API ====
def call_album_v2_exact(album_id, web_bearer, client_token):
    headers = {
        "Authorization": f"Bearer {web_bearer}",
        "Client-Token": client_token,
        "User-Agent": USER_AGENT,
        "App-Platform": "Web Player",
        "content-type": "application/json",
    }
    vars_copy = dict(CAPTURED_VARS)
    vars_copy["uri"] = f"spotify:album:{album_id}"
    body = {
        "variables": vars_copy,
        "extensions": {"persistedQuery": {"version": 1, "sha256Hash": PERSISTED_HASH}},
        "operationName": OPERATION_NAME,
    }
    for host in PATHFINDER_HOSTS:
        url = f"https://{host}/pathfinder/v2/query"
        try:
            r = requests.post(url, headers=headers, json=body)
            if r.status_code == 200:
                return r.json()
        except: continue
    raise RuntimeError("All album fetch attempts failed")

# ==== Main ====
async def main():
    cat_rows = airtable_list_catalogue_isrcs()
    if not cat_rows:
        print("No ISRCs found.")
        return

    seen, isrc_order, cat_id_by_isrc = set(), [], {}
    for r in cat_rows:
        isrc = r["isrc"]
        if isrc not in seen:
            seen.add(isrc)
            isrc_order.append(isrc)
            cat_id_by_isrc[isrc] = r["id"]

    print(f"Running for {len(isrc_order)} ISRC(s)...")
    web_bearer, client_token = await sniff_tokens_in_browser()
    search_bearer = get_search_token()

    s = requests.Session()
    track_album_by_isrc, album_cache = {}, {}
    for isrc in isrc_order:
        try:
            t = search_track_by_isrc(isrc, search_bearer)
            if t: track_album_by_isrc[isrc] = t
            else: track_album_by_isrc[isrc] = (None, None, "Not found")
        except Exception as e:
            track_album_by_isrc[isrc] = (None, None, str(e))

    fetched_albums = set()
    for isrc, (tid, aid, _) in track_album_by_isrc.items():
        if aid and aid not in fetched_albums:
            time.sleep(SPOTIFY_SLEEP_BETWEEN_ALBUMS)
            album_cache[aid] = call_album_v2_exact(aid, web_bearer, client_token)
            fetched_albums.add(aid)

    today_iso = date.today().isoformat()
    rows, inserts = [], []

    for isrc in isrc_order:
        tid, aid, tname = track_album_by_isrc.get(isrc, (None, None, None))
        row = {"isrc": isrc, "track_name": tname, "play_count": None, "error": None}
        if not aid or not tid:
            row["error"] = "No album/track ID"
            rows.append(row)
            continue

        js = album_cache.get(aid)
        pc = parse_playcount(js, tid)
        if pc is None:
            row["error"] = "Playcount missing"
            rows.append(row)
            continue

        row["play_count"] = pc
        rows.append(row)

        prev_pc = airtable_get_previous_playcount(cat_id_by_isrc[isrc])
        delta = pc - prev_pc if isinstance(prev_pc, int) else None
        inserts.append({
            "catalogue_rec_id": cat_id_by_isrc[isrc],
            "date_iso": today_iso,
            "playcount": pc,
            "delta": delta,
        })

    with open("playcounts.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["isrc", "track_name", "play_count", "error"])
        w.writeheader(); w.writerows(rows)
    print("Wrote playcounts.csv")

    if inserts:
        airtable_batch_insert_playcounts(inserts)
        print("Inserted rows into Airtable.")

    for r in rows:
        print(r)

if __name__ == "__main__":
    asyncio.run(main())
