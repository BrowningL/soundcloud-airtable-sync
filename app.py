import asyncio
import os
from datetime import datetime
import aiohttp
import time
from pyairtable import Table
from pyairtable.formulas import match

from playwright.async_api import async_playwright

AIRTABLE_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("AIRTABLE_BASE_ID")
CATALOGUE_TABLE = "Catalogue"
VIEW_NAME = "Inner Catalogue"
OUTPUT_TABLE = "SpotifyPlaycounts"

HEADERS = {
    "App-Platform": "WebPlayer",
    "Client-Token": "",  # filled in dynamically
    "Authorization": "",  # filled in dynamically
}

async def get_tokens():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto("https://open.spotify.com/album/5VdyJkLe3yvOs0l4xXbWp0")
        await page.wait_for_timeout(3000)
        cookies = await context.cookies()
        local_storage = await context.evaluate("() => Object.assign({}, window.localStorage)")
        bearer = local_storage.get("accessToken")
        client = local_storage.get("clientToken")
        await browser.close()
        return bearer, client

async def fetch_playcount(session, bearer, client_token, isrc):
    url = "https://api-partner.spotify.com/pathfinder/v1/query"
    payload = {
        "operationName": "getTrackMetadata",
        "variables": {"uri": f"spotify:track:{isrc}"},
        "extensions": {"persistedQuery": {"version": 1, "sha256Hash": "placeholder"}}
    }
    headers = {
        "Authorization": f"Bearer {bearer}",
        "Client-Token": client_token,
        "App-Platform": "WebPlayer"
    }
    async with session.post(url, json=payload, headers=headers) as r:
        data = await r.json()
        try:
            count = int(data['data']['trackUnion']['streamCount'])
            return count
        except Exception:
            return None

async def main():
    bearer, client_token = await get_tokens()
    HEADERS["Authorization"] = f"Bearer {bearer}"
    HEADERS["Client-Token"] = client_token

    catalogue = Table(AIRTABLE_KEY, BASE_ID, CATALOGUE_TABLE)
    output = Table(AIRTABLE_KEY, BASE_ID, OUTPUT_TABLE)

    rows = catalogue.all(view=VIEW_NAME)
    today = datetime.utcnow().strftime("%Y-%m-%d")

    async with aiohttp.ClientSession() as session:
        for row in rows:
            isrc = row["fields"].get("ISRC")
            if not isrc:
                continue
            playcount = await fetch_playcount(session, bearer, client_token, isrc)
            if not playcount:
                continue

            existing = output.all(formula=match({"ISRC": [row["id"]]}))
            prev_count = None
            if existing:
                recent = sorted(
                    [r for r in existing if "Playcount" in r["fields"]],
                    key=lambda x: x["fields"].get("Date", ""),
                    reverse=True,
                )
                if recent:
                    prev_count = recent[0]["fields"]["Playcount"]

            delta = playcount - prev_count if prev_count is not None else None
            output.create({
                "ISRC": [row["id"]],
                "Date": today,
                "Playcount": playcount,
                "Delta": delta if delta is not None else "n/a"
            })

            await asyncio.sleep(1)  # delay to avoid hammering

if __name__ == "__main__":
    asyncio.run(main())
