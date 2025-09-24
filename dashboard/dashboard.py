# dashboard.py — KAIZEN-styled Flask dashboard (Streams & Playlist Growth + Catalogue)
import os
import time
import logging
from datetime import date, datetime, timedelta
from typing import List, Tuple, Dict, Any, Optional
from collections import defaultdict
import json
import urllib.parse
import urllib.request

from flask import Flask, jsonify, render_template_string, request

# ── Config ────────────────────────────────────────────────────────────────────
LOCAL_TZ = os.getenv("LOCAL_TZ", "Europe/London")
DEFAULT_PLAYLIST_NAME = os.getenv("DEFAULT_PLAYLIST_NAME", "TOGI Motivation")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

# Airtable (for catalogue size)
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Catalogue")
AIRTABLE_RELEASE_DATE_FIELD = os.getenv("AIRTABLE_RELEASE_DATE_FIELD", "Release Date")
AIRTABLE_DISTRIBUTOR_FIELD = os.getenv("AIRTABLE_DISTRIBUTOR_FIELD", "Distributor")
AIRTABLE_EXCLUDED_DISTRIBUTOR = os.getenv("AIRTABLE_EXCLUDED_DISTRIBUTOR", "External")
AIRTABLE_CACHE_TTL_SECS = int(os.getenv("AIRTABLE_CACHE_TTL_SECS", "21600"))  # 6h

# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger("dashboard")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger.setLevel(logging.INFO)

app = Flask(__name__)  # serves /static/* by default

# ── DB access (pool with circuit-breaker to fresh connections) ────────────────
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool, PoolTimeout, PoolClosed
from contextlib import suppress
import random

_URL_HAS_SSLMODE = ("sslmode=" in DATABASE_URL.lower())
DEFAULT_SSLMODE = None if _URL_HAS_SSLMODE else os.getenv("DB_SSLMODE", "prefer").lower()

DB_CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT_SECS", "10"))
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "5"))
DB_TIMEOUT_SECS = int(os.getenv("DB_TIMEOUT_SECS", "10"))
DB_MAX_LIFETIME_SECS = int(os.getenv("DB_MAX_LIFETIME_SECS", "240"))
DB_MAX_IDLE_SECS = int(os.getenv("DB_MAX_IDLE_SECS", "90"))
# If set, always bypass pool:
DB_NO_POOL = os.getenv("DB_NO_POOL", "0") == "1"

# After we see a fatal socket error, bypass the pool for this long:
DIRECT_MODE_COOLDOWN_SECS = int(os.getenv("DB_DIRECT_COOLDOWN_SECS", "300"))

_DIRECT_MODE_UNTIL: float | None = None  # epoch seconds; None = pool mode

FATAL_IO_SNIPPETS = (
    "consuming input failed",
    "server closed the connection unexpectedly",
    "connection already closed",
    "connection not open",
)

def _now() -> float:
    return time.time()

def _pool_kwargs(sslmode: Optional[str]) -> Dict[str, Any]:
    # Apply statement timeouts at connect time (no warm-up SETs)
    kwargs: Dict[str, Any] = {
        "options": "-c statement_timeout=20000 -c idle_in_transaction_session_timeout=0",
        "connect_timeout": DB_CONNECT_TIMEOUT,
        # TCP keepalives help detect half-open sockets
        "keepalives": 1, "keepalives_idle": 30, "keepalives_interval": 10, "keepalives_count": 5,
    }
    if sslmode:
        kwargs["sslmode"] = sslmode  # 'require'|'prefer'|'disable'
    return kwargs

def _build_pool(sslmode: Optional[str]) -> ConnectionPool:
    base_args = dict(
        conninfo=DATABASE_URL,
        kwargs=_pool_kwargs(sslmode),
        min_size=1,
        max_size=DB_POOL_MAX,
        timeout=DB_TIMEOUT_SECS,
        max_lifetime=DB_MAX_LIFETIME_SECS,
        max_idle=DB_MAX_IDLE_SECS,
        open=False,
    )
    try:
        return ConnectionPool(reconnect=True, **base_args)  # psycopg_pool >=3.2
    except TypeError:
        return ConnectionPool(**base_args)

POOL = _build_pool(DEFAULT_SSLMODE)

def _rebuild_pool(force_sslmode: Optional[str] = None):
    global POOL
    with suppress(Exception):
        POOL.close()
    time.sleep(0.25)
    sslmode = force_sslmode if force_sslmode is not None else DEFAULT_SSLMODE
    POOL = _build_pool(sslmode)

def _ensure_pool_open_adaptive():
    try:
        POOL.open()
    except Exception as e:
        msg = str(e).lower()
        if (not _URL_HAS_SSLMODE) and ("server does not support ssl" in msg or "ssl was required" in msg):
            logger.warning("DB reports SSL mismatch; rebuilding pool with sslmode=disable")
            _rebuild_pool("disable")
            POOL.open()
        else:
            raise

def _is_fatal_io_error(e: Exception) -> bool:
    s = str(e).lower()
    return any(sn in s for sn in FATAL_IO_SNIPPETS)

def _enter_direct_mode():
    global _DIRECT_MODE_UNTIL
    _DIRECT_MODE_UNTIL = _now() + DIRECT_MODE_COOLDOWN_SECS
    logger.warning("Entering DIRECT DB mode for %d seconds (fresh connection per query).",
                   DIRECT_MODE_COOLDOWN_SECS)

def _direct_query(query: str, params: tuple):
    """
    Fresh connection per call, with retries and jitter.
    Autocommit, no session warm-up.
    """
    kwargs = _pool_kwargs(DEFAULT_SSLMODE)
    attempts = 3
    last_exc: Optional[Exception] = None
    for i in range(1, attempts + 1):
        try:
            with psycopg.connect(DATABASE_URL, **kwargs) as conn:
                conn.autocommit = True
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(query, params)
                    return cur.fetchall() if cur.description else []
        except Exception as e:
            last_exc = e
            if i < attempts and _is_fatal_io_error(e):
                # small backoff with jitter
                time.sleep(0.20 + random.random() * 0.30)
                continue
            break
    if last_exc:
        raise last_exc
    raise RuntimeError("Direct query failed unexpectedly")

def _pooled_query(query: str, params: tuple):
    """
    Pool path with aggressive discard on any failure.
    """
    exc: Optional[Exception] = None
    for attempt in (1, 2, 3):
        try:
            _ensure_pool_open_adaptive()
            with POOL.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(query, params)
                    return cur.fetchall() if cur.description else []
        except (psycopg.OperationalError, psycopg.InterfaceError, PoolTimeout, PoolClosed) as e:
            exc = e
            logger.warning("DB pooled op failed (%s). Attempt %d/3", e.__class__.__name__, attempt)
            if _is_fatal_io_error(e):
                _enter_direct_mode()
                # Immediately fail over to direct mode for this call
                return _direct_query(query, params)
            if attempt == 1:
                with suppress(Exception): POOL.close()
                time.sleep(0.25)
                continue
            elif attempt == 2:
                _rebuild_pool()
                with suppress(Exception): POOL.open()
                time.sleep(0.35)
                continue
            else:
                break
        except Exception as e:
            exc = e
            # Non-recoverable SQL/logic errors
            break
    if exc:
        # If pooled path failed repeatedly, switch to direct mode and try once
        if _is_fatal_io_error(exc):
            _enter_direct_mode()
            return _direct_query(query, params)
        raise exc
    raise RuntimeError("Pooled query failed unexpectedly")

def _q(query: str, params: tuple | None = None):
    """
    Unified read-only query entrypoint.
    - If DB_NO_POOL=1: always direct.
    - If in cooldown window: direct.
    - Else: pooled; any fatal I/O flips to direct for a while.
    """
    params = params or ()
    if DB_NO_POOL:
        return _direct_query(query, params)
    if _DIRECT_MODE_UNTIL and _now() < _DIRECT_MODE_UNTIL:
        return _direct_query(query, params)
    try:
        return _pooled_query(query, params)
    except Exception as e:
        # As an ultimate fallback, try direct once more
        if _is_fatal_io_error(e):
            _enter_direct_mode()
            return _direct_query(query, params)
        raise


# ── Series helpers ────────────────────────────────────────────────────────────
def _daterange(start: date, end: date) -> List[date]:
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]

def _fill_series(rows: List[Tuple[date, int]], start: date, end: date):
    idx = {d: int(v) for d, v in rows}
    labels, values = [], []
    for d in _daterange(start, end):
        labels.append(d.isoformat())
        values.append(idx.get(d, 0))
    return labels, values

def _clamp_days(raw: str | None, default: int = 90, min_d: int = 1, max_d: int = 365) -> int:
    try:
        v = int(raw or default)
    except Exception:
        v = default
    return min(max(v, min_d), max_d)

# ── Airtable helpers ──────────────────────────────────────────────────────────
def _airtable_enabled() -> bool:
    return bool(AIRTABLE_API_KEY and AIRTABLE_BASE_ID)

_airtable_cache: Dict[str, Any] = {
    "at": None, "labels": None, "values": None,
    "min_date": None, "max_date": None, "count": 0,
    "ok": False, "error": None
}

def _http_get_json(url: str, headers: Dict[str, str], params: Dict[str, Any], timeout: float = 30.0) -> Dict[str, Any]:
    query = urllib.parse.urlencode(
        [(k, v) for k, vv in params.items() for v in (vv if isinstance(vv, list) else [vv])]
    )
    full = f"{url}?{query}" if query else url
    req = urllib.request.Request(full, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

def _fetch_catalogue_cumulative():
    now = datetime.utcnow()
    if _airtable_cache["at"] and (now - _airtable_cache["at"]).total_seconds() < AIRTABLE_CACHE_TTL_SECS:
        return (
            _airtable_cache["labels"], _airtable_cache["values"],
            _airtable_cache["min_date"], _airtable_cache["max_date"],
            _airtable_cache["count"], _airtable_cache["ok"], _airtable_cache["error"]
        )

    if not _airtable_enabled():
        _airtable_cache.update({"at": now, "labels": [], "values": [], "min_date": None,
                                "max_date": None, "count": 0, "ok": False, "error": "Airtable not configured"})
        return [], [], None, None, 0, False, "Airtable not configured"

    base_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{urllib.parse.quote(AIRTABLE_TABLE_NAME)}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}

    formula = f"AND({{{AIRTABLE_RELEASE_DATE_FIELD}}}, NOT({{{AIRTABLE_DISTRIBUTOR_FIELD}}} = '{AIRTABLE_EXCLUDED_DISTRIBUTOR}'))"

    params = {
        "pageSize": 100,
        "fields[]": AIRTABLE_RELEASE_DATE_FIELD,
        "filterByFormula": formula,
    }

    per_day_additions: Dict[date, int] = defaultdict(int)
    total = 0
    seen_min: Optional[date] = None
    seen_max: Optional[date] = None

    offset = None
    try:
        while True:
            q = dict(params)
            if offset:
                q["offset"] = offset
            data = _http_get_json(base_url, headers, q, timeout=30.0)
            records = data.get("records", [])
            for rec in records:
                raw = (rec.get("fields", {}) or {}).get(AIRTABLE_RELEASE_DATE_FIELD)
                if not raw:
                    continue
                try:
                    d = date.fromisoformat(raw[:10])
                except Exception:
                    try:
                        d = datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
                    except Exception:
                        continue
                per_day_additions[d] += 1
                total += 1
                seen_min = d if seen_min is None or d < seen_min else seen_min
                seen_max = d if seen_max is None or d > seen_max else seen_max

            offset = data.get("offset")
            if not offset:
                break

        if total == 0 or not seen_min or not seen_max:
            _airtable_cache.update({"at": now, "labels": [], "values": [],
                                    "min_date": None, "max_date": None, "count": 0, "ok": True, "error": None})
            return [], [], None, None, 0, True, None

        start = seen_min
        end = max(seen_max, date.today())
        labels, values = [], []
        running = 0
        for d in _daterange(start, end):
            running += per_day_additions.get(d, 0)
            labels.append(d.isoformat())
            values.append(running)

        _airtable_cache.update({
            "at": now, "labels": labels, "values": values,
            "min_date": start, "max_date": end, "count": total, "ok": True, "error": None
        })
        return labels, values, start, end, total, True, None

    except Exception as e:
        _airtable_cache.update({"at": now, "labels": [], "values": [],
                                "min_date": None, "max_date": None, "count": 0, "ok": False, "error": str(e)})
        return [], [], None, None, 0, False, str(e)

# ── UI ────────────────────────────────────────────────────────────────────────
@app.get("/")
def ui():
    html = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" /><meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>KAIZEN — Streams & Playlists</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/chartjs-chart-matrix@^2/dist/chartjs-chart-matrix.min.js"></script>
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="stylesheet" href="/static/styles.css" />
  <style>
    /* Force a clean system font everywhere */
    :root { --ui-font: -apple-system, system-ui, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji","Segoe UI Emoji"; }
    html, body { height: 100%; }
    body {
      margin: 0;
      background: #fff;
      color: #111;
      font-family: var(--ui-font);
      -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale;
    }
    /* Ensure ALL elements use the system font */
    *, *::before, *::after,
    button, input, select, textarea, label, table, th, td, h1, h2, h3, h4, h5, h6 {
      font-family: var(--ui-font) !important;
    }

    /* Frame & layout */
    .fixed-frame { position: fixed; inset: 0; pointer-events: none; border: 18px solid #000; box-sizing: border-box; z-index: 9999; }
    .content { padding: 24px; }
    .brand-header { display:flex; align-items:center; justify-content:space-between; margin-bottom: .75rem; }
    .brand-title { font-weight: 800; letter-spacing: -0.02em; }
    .card { background: #fff; border-radius: 16px; box-shadow: 0 10px 28px rgba(0,0,0,.08); padding: 1.1rem; }
    h2 { font-size: 1.125rem; font-weight: 600; }
    table { width:100%; border-collapse: collapse; }
    th, td { padding:.5rem; border-bottom:1px solid rgba(0,0,0,.06); }
    .scroll { max-height: 420px; overflow:auto; }
    .kaizen-bold { font-family: var(--ui-font) !important; font-weight: 700; }

    /* Optional dark background for the container; charts keep defaults */
    @media (prefers-color-scheme: dark) {
      body { background:#111; color:#f5f5f5; }
      .card { background: rgba(24,24,27,.82); color:#fff; }
      th, td { border-color: rgba(255,255,255,.08); }
      a { color: #f87171; }
    }

    @media (max-width: 900px) and (orientation: landscape) {
      .content { padding: 12px; }
      .card { padding: .85rem; border-radius: 12px; }
      h2 { font-size: 1rem; }
      .brand-title { font-size: 1rem; }
      canvas { max-height: 240px; }
    }
  </style>
</head>
<body>
  <div class="fixed-frame"></div>

  <div class="content max-w-7xl mx-auto">
    <header class="brand-header">
      <div class="brand-title text-xl sm:text-2xl">Catalogue Dashboard</div>
    </header>

    <section class="grid grid-cols-1 gap-6">
      <div class="card">
        <div class="flex items-center justify-between mb-3">
          <h2>Daily Streams</h2>
          <div>
            <label class="mr-2 text-sm opacity-70">Window</label>
            <select id="streamsDays" class="border rounded px-2 py-1 text-sm">
              <option value="30">30 days</option>
              <option value="90" selected>90 days</option>
              <option value="180">180 days</option>
            </select>
          </div>
        </div>
        <canvas id="streamsChart" height="110"></canvas>
      </div>
    </section>

    <section class="grid grid-cols-1 lg:grid-cols-[320px,1fr] gap-6 mt-6">
      <div class="card">
        <h2 class="mb-3">Playlists</h2>
        <label class="text-sm opacity-70">Select playlist</label>
        <select id="playlistSelect" class="kaizen-bold w-full border rounded px-2 py-2 mt-1"></select>
        <div class="mt-4 text-sm">
          <div>Latest followers: <span id="plFollowers" class="font-semibold">-</span></div>
          <div>Last daily Δ: <span id="plDelta" class="font-semibold">-</span></div>
          <div class="mt-2"><a id="plLink" class="underline" target="_blank" rel="noopener">Open in Spotify</a></div>
        </div>
      </div>

      <div class="card">
        <div class="flex items-center justify-between mb-3">
          <h2>Playlist Growth</h2>
          <div>
            <label class="mr-2 text-sm opacity-70">Window</label>
            <select id="playlistDays" class="border rounded px-2 py-1 text-sm">
              <option value="30">30 days</option>
              <option value="90" selected>90 days</option>
              <option value="180">180 days</option>
            </select>
          </div>
        </div>
        <canvas id="playlistChart" height="120"></canvas>
      </div>
    </section>

    <section class="grid grid-cols-1 gap-6 mt-6">
      <div class="card">
        <div class="flex items-center justify-between mb-3">
          <h2>Catalogue Size Over Time</h2>
          <div class="text-sm opacity-70" id="catalogueMeta"></div>
        </div>
        <canvas id="catalogueChart" height="120"></canvas>
      </div>
    </section>

    <section class="grid grid-cols-1 gap-6 mt-6">
        <div class="card">
            <h2 class="mb-3">Catalogue Status Heatmap</h2>
            <div id="healthHeatmapContainer" class="w-full h-[600px] overflow-x-auto overflow-y-auto relative">
                <canvas id="healthHeatmapChart"></canvas>
            </div>
        </div>
    </section>

    <section class="grid grid-cols-1 gap-6 mt-6">
      <div class="card">
        <div class="flex items-center justify-between mb-3">
          <h2>Best Artists Today — Share of Total Streams Δ</h2>
          <div class="text-sm opacity-70" id="bestArtistsDateLabel"></div>
        </div>
        <canvas id="bestArtistsChart" height="120"></canvas>
      </div>
    </section>

    <section class="grid grid-cols-1 gap-6 mt-6">
      <div class="card">
        <div class="flex items-center justify-between mb-3">
          <h2>Best tracks today - Track deltas by day</h2>
          <div class="flex items-center gap-2">
            <label class="text-sm opacity-70">Date</label>
            <select id="deltaDate" class="border rounded px-2 py-1 text-sm"></select>
            <button id="btnReloadDeltas" class="border rounded px-3 py-1 text-sm">Reload</button>
          </div>
        </div>
        <div class="scroll">
          <table>
            <thead>
              <tr><th class="text-left">#</th><th class="text-left">ISRC</th><th class="text-left">Title</th><th class="text-left">Artist</th><th class="text-right">Δ</th></tr>
            </thead>
            <tbody id="deltaTableBody"></tbody>
          </table>
        </div>
      </div>
    </section>
  </div>

<script>
  const DEFAULT_PLAYLIST_NAME = {{ default_playlist_name | tojson }};
  let streamsChart, playlistChart, bestArtistsChart, catalogueChart, healthHeatmapChart;
  const fmt = (n) => Number(n).toLocaleString();
  async function api(path) { const r = await fetch(path); if (!r.ok) throw new Error(await r.text()); return r.json(); }

  // Streams (Chart.js defaults)
  async function loadStreams(days) {
    const data = await api('/api/streams/total-daily?days=' + days);
    const ctx = document.getElementById('streamsChart').getContext('2d');
    const cfg = {
      type: 'line',
      data: { labels: data.labels, datasets: [{ label: 'Streams Δ (sum)', data: data.values, tension: 0.3, fill: false }] },
      options: { responsive: true, maintainAspectRatio: true,
                 scales: { x: { ticks: { maxRotation: 0, autoSkip: true } }, y: { beginAtZero: true } },
                 plugins: { tooltip: { callbacks: { label: (c) => ' ' + fmt(c.parsed.y) } } } }
    };
    if (streamsChart) streamsChart.destroy(); streamsChart = new Chart(ctx, cfg);
  }

  // Playlists list
  async function loadPlaylists() {
    const list = await api('/api/playlists/list');
    const sel = document.getElementById('playlistSelect'); sel.innerHTML = '';
    let defaultId = list.length ? list[0].playlist_id : null;
    for (const p of list) { const opt = document.createElement('option'); opt.value = p.playlist_id; opt.textContent = p.playlist_name || p.playlist_id; sel.appendChild(opt); }
    const def = list.find(p => (p.playlist_name || '').toLowerCase().startsWith(DEFAULT_PLAYLIST_NAME.toLowerCase()));
    if (def) defaultId = def.playlist_id; if (defaultId) sel.value = defaultId;
    await updatePlaylistCard(); await loadPlaylistChart(document.getElementById('playlistDays').value);
  }

  async function updatePlaylistCard() {
    const list = await api('/api/playlists/list');
    const id = document.getElementById('playlistSelect').value; const p = list.find(x => x.playlist_id === id); if (!p) return;
    document.getElementById('plFollowers').textContent = fmt(p.followers ?? 0);
    document.getElementById('plDelta').textContent = (p.delta == null) ? '-' : fmt(p.delta);
    document.getElementById('plLink').href = p.web_url;
  }

  // Playlist Growth (defaults)
  async function loadPlaylistChart(days) {
    const id = document.getElementById('playlistSelect').value; if (!id) return;
    const data = await api('/api/playlists/' + encodeURIComponent(id) + '/series?days=' + days);
    const ctx = document.getElementById('playlistChart').getContext('2d');
    const cfg = {
      data: { labels: data.labels, datasets: [
        { type: 'line', label: 'Followers', data: data.followers, yAxisID: 'y1', tension: 0.25 },
        { type: 'bar',  label: 'Daily Δ',  data: data.deltas,    yAxisID: 'y2' }
      ]},
      options: { responsive: true, maintainAspectRatio: true,
        scales: {
          x: { ticks: { maxRotation: 0, autoSkip: true } },
          y1: { type: 'linear', position: 'left', beginAtZero: true },
          y2: { type: 'linear', position: 'right', beginAtZero: true, grid: { drawOnChartArea: false } }
        },
        plugins: { tooltip: { callbacks: { label: (c) => ' ' + fmt(c.parsed.y) } } }
      }
    };
    if (playlistChart) playlistChart.destroy(); playlistChart = new Chart(ctx, cfg); await updatePlaylistCard();
  }

  // Best artists (defaults)
  async function loadBestArtists() {
    const data = await api('/api/artists/top-share');
    document.getElementById('bestArtistsDateLabel').textContent = 'Date: ' + data.date;
    const ctx = document.getElementById('bestArtistsChart').getContext('2d');
    if (bestArtistsChart) bestArtistsChart.destroy();
    bestArtistsChart = new Chart(ctx, {
      type: 'bar',
      data: { labels: data.labels, datasets: [{ label: 'Share of Daily Streams Δ (%)', data: data.shares }] },
      options: { responsive: true, maintainAspectRatio: true,
        plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => ' ' + c.parsed.y.toFixed(2) + '%' } } },
        scales: { y: { beginAtZero: true, ticks: { callback: (v)=> v + '%' } } } }
    });
  }

  // Catalogue size (defaults)
  async function loadCatalogue() {
    const data = await api('/api/catalogue/size-series');
    const ctx = document.getElementById('catalogueChart').getContext('2d');
    if (catalogueChart) catalogueChart.destroy();
    catalogueChart = new Chart(ctx, {
      type: 'line',
      data: { labels: data.labels, datasets: [{ label: 'Total tracks in catalogue', data: data.values, tension: 0.25, fill: false }] },
      options: { responsive: true, maintainAspectRatio: true,
        scales: { x: { ticks: { maxRotation: 0, autoSkip: true } }, y: { beginAtZero: true } },
        plugins: { tooltip: { callbacks: { label: (c) => ' ' + fmt(c.parsed.y) } } } }
    });
    const meta = document.getElementById('catalogueMeta');
    const total = (data.values && data.values.length) ? data.values[data.values.length-1] : 0;
    meta.textContent = `Total: ${fmt(total)} • Range: ${data.min_date ?? '-'} → ${data.max_date ?? '-'} • Counted: ${fmt(data.count ?? 0)} (excl. 'External')`;
  }

  // Dates + table
  async function loadDeltaDates(days) {
    const data = await api('/api/streams/dates?days=' + days);
    const sel = document.getElementById('deltaDate'); sel.innerHTML = '';
    for (const d of data.dates) { const opt = document.createElement('option'); opt.value = d; opt.textContent = d; sel.appendChild(opt); }
    if (data.dates.length) sel.value = data.dates[0];
  }
  async function loadDeltaTable() {
    const day = document.getElementById('deltaDate').value;
    const data = await api('/api/streams/top-deltas?date=' + encodeURIComponent(day));
    const body = document.getElementById('deltaTableBody'); body.innerHTML = '';
    data.rows.forEach((r, i) => {
      const tr = document.createElement('tr');
      tr.innerHTML = `<td>${i+1}</td><td>${r.isrc ?? ''}</td><td>${r.title ?? ''}</td><td>${r.artist ?? ''}</td><td class="text-right">${fmt(r.delta ?? 0)}</td>`;
      body.appendChild(tr);
    });
  }

  // --- Catalogue Health Heatmap ---
  async function loadHealthHeatmap() {
    const data = await api('/api/catalogue/health-status-heatmap');
    const container = document.getElementById('healthHeatmapContainer');
    if (!data || !data.data || data.data.length === 0) {
        console.warn("No data received for health heatmap.");
        container.innerHTML = '<p class="text-center opacity-70 p-8">No catalogue health data available to display.</p>';
        return;
    }

    const ctx = document.getElementById('healthHeatmapChart').getContext('2d');
    const statusMap = {
        3: { name: 'Exists on Apple Music and Spotify', color: 'limegreen' },
        2: { name: 'Exists on Apple Music only', color: 'lightcoral' },
        1: { name: 'Exists on Spotify only', color: '#006400' },
        0: { name: 'Does Not Exist', color: '#dc2626' }
    };

    // Dynamically calculate canvas dimensions
    const boxHeight = 12; // Height of each track row
    const boxWidth = 20;  // Width of each date column
    const yLabelsCount = Math.max(data.yLabels.length, data.catalogueTotalSize);
    
    const canvasHeight = yLabelsCount * boxHeight;
    const canvasWidth = data.xLabels.length * boxWidth;

    const chartCanvas = document.getElementById('healthHeatmapChart');
    chartCanvas.height = canvasHeight;
    chartCanvas.width = canvasWidth;
    
    const cfg = {
        type: 'matrix',
        data: {
            datasets: [{
                label: 'Catalogue Status',
                data: data.data,
                backgroundColor: (c) => statusMap[c.raw.v]?.color || 'lightgray',
                borderColor: (c) => statusMap[c.raw.v]?.color || 'lightgray',
                borderWidth: 1,
                width: boxWidth - 2,
                height: boxHeight - 2,
            }]
        },
        options: {
            responsive: false, // Important for scrolling canvas
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        title: (c) => c[0].raw.y,
                        label: (c) => {
                            const status = statusMap[c.raw.v];
                            return `Date: ${c.raw.x}\\nStatus: ${status ? status.name : 'Not Collected'}`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    type: 'category',
                    labels: data.xLabels,
                    ticks: { autoSkip: false, maxRotation: 90, minRotation: 90 },
                    grid: { display: false }
                },
                y: {
                    type: 'category',
                    labels: data.yLabels,
                    offset: true,
                    ticks: { 
                        autoSkip: false,
                        // Show only every Nth label to prevent clutter
                        callback: function(value, index, values) {
                            return (index % 15 === 0 || index === values.length - 1) ? (index+1) : '';
                        },
                        font: { size: 10 }
                    },
                    grid: { display: false },
                    // Set a minimum height based on total catalogue size
                    min: 0,
                    max: yLabelsCount -1,
                }
            }
        }
    };

    if (healthHeatmapChart) healthHeatmapChart.destroy();
    healthHeatmapChart = new Chart(ctx, cfg);
  }

  // Listeners
  document.getElementById('streamsDays').addEventListener('change', e => loadStreams(e.target.value));
  document.getElementById('playlistDays').addEventListener('change', e => loadPlaylistChart(e.target.value));
  document.getElementById('playlistSelect').addEventListener('change', async () => { await updatePlaylistCard(); await loadPlaylistChart(document.getElementById('playlistDays').value); });
  document.getElementById('btnReloadDeltas').addEventListener('click', loadDeltaTable);

  // Bootstrap
  (async () => {
    await loadStreams(document.getElementById('streamsDays').value);
    await loadPlaylists();
    await loadPlaylistChart(document.getElementById('playlistDays').value);
    await loadCatalogue();
    await loadHealthHeatmap();
    await loadBestArtists();
    await loadDeltaDates(90);
    await loadDeltaTable();
  })();
</script>
</body>
</html>"""
    return render_template_string(
        html,
        local_tz=LOCAL_TZ,
        default_playlist_name=DEFAULT_PLAYLIST_NAME
    )

# ── API: streams ──────────────────────────────────────────────────────────────
@app.get("/api/streams/total-daily")
def api_streams_total_daily():
    days = _clamp_days(request.args.get("days"), 90)
    # THIS IS THE CORRECTED SQL QUERY
    q = """
        WITH deltas_per_track_per_day AS (
            SELECT
                s.stream_date,
                s.track_uid,
                GREATEST(0, s.playcount - COALESCE((
                    SELECT s2.playcount
                    FROM streams s2
                    WHERE s2.track_uid = s.track_uid AND s2.stream_date < s.stream_date
                    ORDER BY s2.stream_date DESC
                    LIMIT 1
                ), 0)) AS daily_delta_calc
            FROM streams s
            WHERE s.platform = 'spotify'
              AND s.stream_date >= CURRENT_DATE - %s::int * INTERVAL '1 day'
        )
        SELECT
            stream_date AS d,
            COALESCE(SUM(daily_delta_calc), 0)::bigint AS v
        FROM deltas_per_track_per_day
        GROUP BY stream_date
        ORDER BY stream_date;
    """
    rows = _q(q, (days,))
    if rows:
        start, end = rows[0]["d"], rows[-1]["d"]
    else:
        end = date.today()
        start = end - timedelta(days=days)
    
    # Ensure the date range covers the requested number of days
    expected_start = date.today() - timedelta(days=days)
    if not rows or start > expected_start:
        start = expected_start

    labels, values = _fill_series([(r["d"], r["v"]) for r in rows], start, end)
    return jsonify({"labels": labels, "values": values})

@app.get("/api/streams/dates")
def api_streams_dates():
    days = _clamp_days(request.args.get("days"), 90)
    q = """
        SELECT DISTINCT s.stream_date AS d
        FROM streams s
        WHERE s.platform='spotify'
          AND s.stream_date >= CURRENT_DATE - %s::int * INTERVAL '1 day'
        ORDER BY d DESC
    """
    rows = _q(q, (days,))
    return jsonify({"dates": [r["d"].isoformat() for r in rows]})

@app.get("/api/streams/top-deltas")
def api_streams_top_deltas():
    day = request.args.get("date")
    if not day:
        return jsonify({"rows": []})
    try:
        limit = int(request.args.get("limit", 10000))
    except Exception:
        limit = 10000

    # THIS IS THE CORRECTED SQL QUERY
    q = r"""
        WITH deltas AS (
            SELECT
                s.track_uid,
                GREATEST(0, s.playcount - COALESCE((
                    SELECT s2.playcount
                    FROM streams s2
                    WHERE s2.track_uid = s.track_uid AND s2.stream_date < s.stream_date
                    ORDER BY s2.stream_date DESC
                    LIMIT 1
                ), 0)) AS daily_delta_calc
            FROM streams s
            WHERE s.platform = 'spotify' AND s.stream_date = %s
        )
        SELECT
            t.isrc,
            t.title,
            t.artist,
            d.daily_delta_calc AS delta
        FROM deltas d
        JOIN track_dim t ON t.track_uid = d.track_uid
        WHERE d.daily_delta_calc > 0
        ORDER BY d.daily_delta_calc DESC, t.isrc
        LIMIT %s;
    """
    rows = _q(q, (day, limit))
    out = []
    for r in rows:
        out.append({
            "isrc": r.get("isrc"),
            "title": r.get("title"),
            "artist": r.get("artist"),
            "delta": int(r.get("delta") or 0),
        })
    return jsonify({"rows": out})

# ── API: playlists ────────────────────────────────────────────────────────────
@app.get("/api/playlists/list")
def api_playlists_list():
    q = """
        WITH latest AS (
          SELECT DISTINCT ON (playlist_id)
            playlist_id, playlist_name, followers, snapshot_date
          FROM playlist_followers
          WHERE platform = 'spotify'
          ORDER BY playlist_id, snapshot_date DESC
        )
        SELECT
          l.playlist_id,
          l.playlist_name,
          l.followers,
          d.delta,
          l.snapshot_date AS date,
          'https://open.spotify.com/playlist/' || split_part(l.playlist_id, ':', 3) AS web_url
        FROM latest l
        LEFT JOIN playlist_followers_delta d
          ON d.playlist_id = l.playlist_id AND d.date = l.snapshot_date
        ORDER BY l.followers DESC;
    """
    rows = _q(q)
    out = []
    for r in rows:
        out.append({
            "playlist_id": r["playlist_id"],
            "playlist_name": r["playlist_name"],
            "followers": int(r["followers"]) if r["followers"] is not None else None,
            "delta": int(r["delta"]) if r["delta"] is not None else None,
            "date": r["date"].isoformat() if r["date"] else None,
            "web_url": r["web_url"],
        })
    return jsonify(out)

@app.get("/api/playlists/<path:playlist_id>/series")
def api_playlist_series(playlist_id: str):
    days = _clamp_days(request.args.get("days"), 90)
    q = """
        SELECT date AS d, followers, delta
        FROM playlist_followers_delta
        WHERE playlist_id = %s
          AND date >= CURRENT_DATE - %s::int * INTERVAL '1 day'
        ORDER BY d
    """
    rows = _q(q, (playlist_id, days))
    if rows:
        start, end = rows[0]["d"], rows[-1]["d"]
    else:
        end = date.today(); start = end - timedelta(days=days)
    labels, followers = _fill_series([(r["d"], int(r["followers"] or 0)) for r in rows], start, end)
    _, deltas = _fill_series([(r["d"], int(r["delta"] or 0)) for r in rows], start, end)
    return jsonify({"labels": labels, "followers": followers, "deltas": deltas})

@app.get("/api/playlists/all-series")
def api_playlists_all_series():
    days = _clamp_days(request.args.get("days"), 90)
    q = """
        SELECT playlist_id,
               COALESCE(playlist_name, playlist_id) AS name,
               date AS d,
               followers
        FROM playlist_followers_delta
        WHERE date >= CURRENT_DATE - %s::int * INTERVAL '1 day'
        ORDER BY playlist_id, d
    """
    rows = _q(q, (days,))
    if not rows:
        end = date.today(); start = end - timedelta(days=days)
        return jsonify({"labels": [d.isoformat() for d in _daterange(start, end)], "series": []})

    start, end = rows[0]["d"], rows[-1]["d"]
    labels_base = [d.isoformat() for d in _daterange(start, end)]

    series_map: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        pid = r["playlist_id"]
        series_map.setdefault(pid, {"name": r["name"], "points": []})
        series_map[pid]["points"].append((r["d"], int(r["followers"] or 0)))

    series = []
    for obj in series_map.values():
        _, vals = _fill_series(obj["points"], start, end)
        series.append({"name": obj["name"], "values": vals})

    return jsonify({"labels": labels_base, "series": series})

# Best artists — share of daily delta
@app.get("/api/artists/top-share")
def api_artists_top_share():
    q_latest = "SELECT MAX(stream_date) AS d FROM streams WHERE platform='spotify'"
    r = _q(q_latest)
    latest = r[0]["d"] if r and r[0]["d"] else None
    day = request.args.get("date") or (latest.isoformat() if latest else None)
    if not day:
        return jsonify({"date": None, "labels": [], "values": [], "shares": []})

    # THIS IS THE CORRECTED SQL QUERY
    q = r"""
        WITH deltas_by_track AS (
            SELECT
                s.track_uid,
                GREATEST(0, s.playcount - COALESCE((
                    SELECT s2.playcount
                    FROM streams s2
                    WHERE s2.track_uid = s.track_uid AND s2.stream_date < s.stream_date
                    ORDER BY s2.stream_date DESC
                    LIMIT 1
                ), 0)) AS daily_delta_calc
            FROM streams s
            WHERE s.platform = 'spotify' AND s.stream_date = %s
        ),
        deltas_by_artist AS (
            SELECT
                t.artist,
                d.daily_delta_calc
            FROM deltas_by_track d
            JOIN track_dim t ON t.track_uid = d.track_uid
            WHERE d.daily_delta_calc > 0
        )
        SELECT
            artist,
            COALESCE(SUM(daily_delta_calc), 0)::bigint AS v
        FROM deltas_by_artist
        GROUP BY artist
        HAVING COALESCE(SUM(daily_delta_calc), 0) > 0
        ORDER BY v DESC
        LIMIT 25;
    """
    rows = _q(q, (day,))
    total = sum(int(r["v"]) for r in rows) or 1

    labels = [(r["artist"] or "") for r in rows]
    values = [int(r["v"]) for r in rows]
    shares = [round(v * 100.0 / total, 2) for v in values]
    return jsonify({"date": day, "labels": labels, "values": values, "shares": shares})

# --- API ENDPOINT FOR HEATMAP ---
@app.get("/api/catalogue/health-status-heatmap")
def api_catalogue_health_status_heatmap():
    q = """
        SELECT
            h.check_date,
            t.artist,
            t.title,
            h.apple_music_status,
            h.spotify_status
        FROM catalogue_health_status h
        JOIN track_dim t ON h.track_uid = t.track_uid
        ORDER BY t.artist, t.title, h.check_date;
    """
    rows = _q(q)

    # Fetch total catalogue size to inform the y-axis range
    *_unused, total_count, ok, err = _fetch_catalogue_cumulative()
    if not ok:
        logger.warning(f"Could not fetch Airtable catalogue size for heatmap: {err}")
        total_count = 0 # Fallback

    if not rows:
        return jsonify({"xLabels": [], "yLabels": [], "data": [], "catalogueTotalSize": total_count})

    x_labels = sorted(list(set(r["check_date"].isoformat() for r in rows)))

    track_map = {}
    for r in rows:
        track_key = f"{r['artist'] or 'N/A'} - {r['title'] or 'N/A'}"
        if track_key not in track_map:
            track_map[track_key] = {}
        track_map[track_key][r["check_date"].isoformat()] = {
            "apple": r["apple_music_status"],
            "spotify": r["spotify_status"],
        }

    y_labels = sorted(track_map.keys())
    data_points = []
    for track_name in y_labels:
        for date_str in x_labels:
            status = track_map[track_name].get(date_str)
            if status:
                # 3: Both, 2: Apple only, 1: Spotify only, 0: Neither
                status_code = (1 if status["spotify"] else 0) + (2 if status["apple"] else 0)
                data_points.append({"x": date_str, "y": track_name, "v": status_code})
    
    # If total_count from Airtable is less than what we have in the DB, use the DB count
    if total_count < len(y_labels):
        total_count = len(y_labels)

    return jsonify({
        "xLabels": x_labels,
        "yLabels": y_labels,
        "data": data_points,
        "catalogueTotalSize": total_count
    })

# Catalogue size series API
@app.get("/api/catalogue/size-series")
def api_catalogue_size_series():
    labels, values, min_d, max_d, total, ok, err = _fetch_catalogue_cumulative()
    if not ok:
        return jsonify({"labels": [], "values": [], "error": err}), 200
    return jsonify({
        "labels": labels,
        "values": values,
        "min_date": min_d.isoformat() if isinstance(min_d, date) else None,
        "max_date": max_d.isoformat() if isinstance(max_d, date) else None,
        "count": total
    })

# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    try:
        _q("SELECT NOW()")
        ssl = _q("SHOW ssl")
        db_info = {"ok": True, "ssl": (ssl[0]["ssl"] if ssl else "unknown")}
    except Exception as e:
        db_info = {"ok": False, "error": str(e)}

    at_ok = False
    at_err = None
    if _airtable_enabled():
        *_unused, at_ok, at_err = _fetch_catalogue_cumulative()
    else:
        at_err = "Airtable not configured"

    return {"ok": bool(db_info.get("ok") and (at_ok or not _airtable_enabled())),
            "db": db_info,
            "airtable": {"ok": at_ok, "error": at_err}}

# ── Entrypoint ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, threaded=True)
