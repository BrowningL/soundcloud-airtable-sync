my super cool airtable KPI maker & QOL automator


soundcloud-airtable-sync
Turn Spotify and SoundCloud data into clean, chart-ready rows in Airtable.
This repo ships two small web services:

Spotify service (app.py) — pulls daily Spotify playcounts for each ISRC, writes to Airtable, and computes a safe Delta (daily change) with a one-shot /backfill that fixes gaps or out-of-order inserts.

SoundCloud service (index.js) — downloads a track from SoundCloud, normalizes it to MP3, optionally runs an ACRCloud scan, and attaches the file to your Airtable record via a public URL that’s auto-cleaned once Airtable re-hosts it.

Built to run on Railway. Works locally via Docker or your own Python/Node setup.
