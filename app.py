import os
import json
import time
import asyncio
import threading
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, request
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ==========
# CONFIGURATION (Hardcoded)
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

CLIENT_ID = "0fe3aee6708647338f3fec1673558a8c"
CLIENT_SECRET = "eb12c63ff8a24524af6c5aba83acfb9a"

# GraphQL details
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

# Rate limits
airtable_sleep = 0.2
spotify_sleep = 0.15

# ==========
# Airtable functions
# ==========
def at_headers():
    return {"Authorization": f"Bearer {AT_API_KEY}", "Content-Type": "application/json"}

def at_url(table: str) -> str:
    return f"https://api.airtable.com/v0/{AT_BASE_ID}/{requests.utils.quote(table)}"

def list_isrcs() -> List[Dict[str, str]]:
    rows, offset = [], None
    while True:
        params = {"view": CATALOGUE_VIEW, "pageSize": 100, "fields[]": CATALOGUE_ISRC_FIELD}
        if offset: params["offset"] = offset
        r = requests.get(at_url(CATALOGUE_TABLE), headers=at_headers(), params=params)
        r.raise_for_status()
        data = r.json()
        for rec in data["records"]:
            isrc = rec.get("fields", {}).get(CATALOGUE_ISRC_FIELD)
            if isrc: rows.append({"id": rec["id"], "isrc": isrc})
        offset = data.get("offset")
        if not offset: break
    return rows


def find_today(rec_id: str, today: str) -> Optional[str]:
    formula = (
        f"AND(SEARCH('{rec_id}', ARRAYJOIN({{{PLAYCOUNTS_LINK_FIELD}}})),"
        f"{{{PLAYCOUNTS_DATE_FIELD}}} = '{today}')"
    )
    r = requests.get(at_url(PLAYCOUNTS_TABLE), headers=at_headers(), params={"filterByFormula": formula, "pageSize":1})
    if r.status_code != 200: return None
    recs = r.json().get("records", [])
    return recs[0]["id"] if recs else None


def prev_count(rec_id: str, before: str) -> Optional[int]:
    formula = (
        f"AND(SEARCH('{rec_id}', ARRAYJOIN({{{PLAYCOUNTS_LINK_FIELD}}})),"
        f"{{{PLAYCOUNTS_DATE_FIELD}}} < '{before}')"
    )
    params = {"filterByFormula": formula, "pageSize":1, "sort[0][field]": PLAYCOUNTS_DATE_FIELD, "sort[0][direction]": "desc"}
    r = requests.get(at_url(PLAYCOUNTS_TABLE), headers=at_headers(), params=params)
    if r.status_code!=200: return None
    recs = r.json().get("records", [])
    if not recs: return None
    try: return int(recs[0]["fields"].get(PLAYCOUNTS_COUNT_FIELD, 0))
    except: return None


def upsert_count(rec_id: str, today: str, count: int):
    existing = find_today(rec_id, today)
    prev = prev_count(rec_id, today)
    delta = count - prev if prev is not None else None
    fields = {
        PLAYCOUNTS_LINK_FIELD: [rec_id],
        PLAYCOUNTS_DATE_FIELD: today,
        PLAYCOUNTS_COUNT_FIELD: count,
    }
    if delta is not None: fields[PLAYCOUNTS_DELTA_FIELD] = delta
    payload = {"records": [{"fields": fields}]}
    if existing:
        payload["records"][0]["id"] = existing
        r = requests.patch(at_url(PLAYCOUNTS_TABLE), headers=at_headers(), json=payload)
    else:
        r = requests.post(at_url(PLAYCOUNTS_TABLE), headers=at_headers(), json=payload)
    if r.status_code not in (200,201):
        raise RuntimeError(f"Airtable error {r.status_code}: {r.text}")
    time.sleep(airtable_sleep)

# ==========
# Spotify functions
# ==========
def get_search_token() -> str:
    r = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type":"client_credentials"},
        auth=(CLIENT_ID, CLIENT_SECRET)
    )
    r.raise_for_status()
    return r.json()["access_token"]


def search_track(isrc: str, bearer: str) -> Optional[Tuple[str,str,str]]:
    r = requests.get(
        "https://api.spotify.com/v1/search",
        headers={"Authorization":f"Bearer {bearer}"},
        params={"q":f"isrc:{isrc}","type":"track","limit":5}
    )
    if r.status_code!=200: return None
    for t in r.json().get("tracks",{}).get("items",[]):
        if t.get("external_ids",{}).get("isrc","").upper()==isrc.upper():
            return t["id"], t["album"]["id"], t["name"]
    items = r.json().get("tracks",{}).get("items",[])
    if items: return items[0]["id"], items[0]["album"]["id"], items[0]["name"]
    return None

# ==========
# Token sniff via Playwright
# ==========
async def sniff_tokens() -> Tuple[str, Optional[str]]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent=USER_AGENT)
        page = await ctx.new_page()
        fut = asyncio.get_event_loop().create_future()
        def on_resp(resp):
            if "/pathfinder/v2/query" in resp.url and resp.status==200:
                hdrs = resp.request.headers
                auth = hdrs.get("authorization")
                if auth and auth.startswith("Bearer "):
                    tok = auth.split(" ",1)[1]
                    cli = hdrs.get("client-token")
                    if not fut.done(): fut.set_result((tok, cli))
        page.on("response", on_resp)
        await page.goto("https://open.spotify.com/album/2noRn2Aes5aoNVsU6iWThc")
        try:
            return await asyncio.wait_for(fut, timeout=15)
        finally:
            await browser.close()

# ==========
# Fetch album via GraphQL v2
# ==========
def fetch_album(album_id: str, web_token: str, client_token: Optional[str]) -> Dict[str,Any]:
    sess = requests.Session()
    headers = {
        "Authorization":f"Bearer {web_token}",
        "User-Agent": USER_AGENT,
        "content-type":"application/json"
    }
    if client_token: headers["Client-Token"] = client_token
    body = {"operationName":OPERATION_NAME,
            "variables":{**CAPTURED_VARS, "uri":f"spotify:album:{album_id}"},
            "extensions":{"persistedQuery":{"version":1,"sha256Hash":PERSISTED_HASH}}}
    for host in PATHFINDER_HOSTS:
        try:
            r = sess.post(f"https://{host}/pathfinder/v2/query", headers=headers, json=body, timeout=30)
            if r.status_code==200: return r.json()
        except:
            continue
    return {}

# ==========
# Main synchronization
# ==========
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
    cache = {}
    today = date.today().isoformat()

    for isrc in order:
        tr = search_track(isrc, search_tok)
        if not tr:
            upsert_count(linkmap[isrc], today, 0)
            continue
        tid, aid, _ = tr
        if aid not in cache:
            time.sleep(spotify_sleep)
            cache[aid] = fetch_album(aid, web_tok, cli_tok)
        pc = 0
        js = cache[aid]
        # v2
        for it in js.get("data",{}).get("albumUnion",{}).get("tracksV2",{}).get("items",[]):
            track = it.get("track",{})
            if tid in (track.get("id"), track.get("uri","").split(":")[-1]):
                val = track.get("playcount") or 0
                pc = int(str(val).replace(",",""))
                break
        upsert_count(linkmap[isrc], today, pc)
    print("Completed.")

# ==========
# Flask endpoints
# ==========
app = Flask(__name__)

@app.get("/health")
def health(): return jsonify({"ok":True})

@app.post("/run")
def run_endpoint():
    async_flag = request.args.get("async","0") in ("1","true")
    if async_flag:
        threading.Thread(target=lambda: asyncio.run(sync()), daemon=True).start()
        return jsonify({"status":"started"}), 202
    else:
        asyncio.run(sync())
        return jsonify({"status":"completed"}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT","3000"))
    app.run(host="0.0.0.0", port=port)
