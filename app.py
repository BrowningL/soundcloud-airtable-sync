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

AIRTABLE_SLEEP_BETWEEN_WRITES = 0.2    # seconds between Airtable batch writes
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
    # Airtable expects URL-encoded table name if it has spaces
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

def airtable_get_previous_playcount(catalogue_rec_id: str) -> Optional[int]:
    """
    Reads latest (by Date desc) playcount row for a given linked Catalogue record.
    """
    url = _at_base_url(PLAYCOUNTS_TABLE)
    # Formula finds rows whose linked field (array) contains the Catalogue record id
    formula = f"SEARCH('{catalogue_rec_id}', ARRAYJOIN({{{{{ {PLAYCOUNTS_LINK_FIELD} }}}}}))"
    params = {
        "filterByFormula": formula,
        "pageSize": 1,
        "sort[0][field]": PLAYCOUNTS_DATE_FIELD,
        "sort[0][direction]": "desc",
    }
    r = requests.get(url, headers=_at_headers_bearer(), params=params, timeout=30)
    if r.status_code != 200:
        return None
    records = r.json().get("records", [])
    if not records:
        return None
    fields = records[0].get("fields", {}) or {}
    val = fields.get(PLAYCOUNTS_COUNT_FIELD)
    try:
        return int(val)
    except Exception:
        return None

def airtable_batch_insert_playcounts(rows: List[Dict[str, Any]]) -> None:
    """
    Inserts rows into SpotifyPlaycounts. Each row must contain:
      - "catalogue_rec_id": str (linked record id)
      - "date_iso": str (YYYY-MM-DD)
      - "playcount": int
      - "delta": Optional[int]
    """
    if not rows:
        return
    url = _at_base_url(PLAYCOUNTS_TABLE)

    def to_record(r: Dict[str, Any]) -> Dict[str, Any]:
        fields = {
            PLAYCOUNTS_LINK_FIELD: [r["catalogue_rec_id"]],
            PLAYCOUNTS_DATE_FIELD: r["date_iso"],
            PLAYCOUNTS_COUNT_FIELD: r["playcount"],
        }
        if r.get("delta") is not None:
            fields[PLAYCOUNTS_DELTA_FIELD] = r["delta"]
        return {"fields": fields}

    # Chunk insert (<=10 records/request recommended)
    for i in range(0, len(rows), 10):
        chunk = rows[i:i+10]
        body = {"records": [to_record(x) for x in chunk]}
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
        # Treat as not found; log in calling layer if needed
        return None
    items = (r.json().get("tracks") or {}).get("items") or []
    if not items:
        return None
    t = items[0]
    return t["id"], t["album"]["id"], t["name"]

def parse_playcount(js: Dict[str, Any], track_id: str) -> Optional[int]:
    # v2 schema (albumUnion -> tracksV2 -> items -> track.playcount)
    try:
        items = js["data"]["albumUnion"]["tracksV2"]["items"]
        for it in items:
            tr = it.get("track") or {}
            # track id may appear as id or in uri
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
    # fallback: older schema (rare now)
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
                if isin
