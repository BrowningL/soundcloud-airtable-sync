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

# ── DB pool (adaptive SSL + reconnect, upgraded with retries) ──────────────────
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool, PoolTimeout, PoolClosed

_URL_HAS_SSLMODE = ("sslmode=" in DATABASE_URL.lower())

def _build_pool(sslmode: Optional[str]) -> ConnectionPool:
    kwargs: Dict[str, Any] = {}
    if sslmode:
        kwargs["sslmode"] = sslmode  # 'require' | 'prefer' | 'disable'
    kwargs.update({
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    })
    return ConnectionPool(
        conninfo=DATABASE_URL,
        kwargs=kwargs,
        min_size=1,
        max_size=int(os.getenv("DB_POOL_MAX", "5")),
        timeout=int(os.getenv("DB_TIMEOUT_SECS", "10")),  # wait for free conn
        max_lifetime=int(os.getenv("DB_MAX_LIFETIME_SECS", "300")),   # recycle faster
        max_idle=int(os.getenv("DB_MAX_IDLE_SECS", "120")),           # recycle idle conns
        open=False,  # lazy-open
    )

DEFAULT_SSLMODE = None if _URL_HAS_SSLMODE else os.getenv("DB_SSLMODE", "prefer").lower()
POOL = _build_pool(DEFAULT_SSLMODE)

def _rebuild_pool(force_sslmode: Optional[str] = None):
    global POOL
    try:
        POOL.close()
    except Exception:
        pass
    time.sleep(0.25)
    sslmode = force_sslmode if force_sslmode is not None else DEFAULT_SSLMODE
    POOL = _build_pool(sslmode)

def _ensure_pool_open_adaptive():
    global POOL
    try:
        POOL.open()
        return
    except Exception as e:
        msg = str(e).lower()
        if (not _URL_HAS_SSLMODE) and ("server does not support ssl" in msg or "ssl was required" in msg):
            logger.warning("DB reports SSL mismatch; rebuilding pool with sslmode=disable")
            _rebuild_pool("disable")
            POOL.open()
            return
        raise

def _warm_conn(conn: psycopg.Connection):
    """Run lightweight safety commands to ensure the connection is alive."""
    conn.execute("SET statement_timeout = 20000")
    conn.execute("SET idle_in_transaction_session_timeout = 0")
    conn.execute("SELECT 1")

def _soft_reopen_pool():
    try:
        POOL.close()
    except Exception:
        pass
    time.sleep(0.3)
    _ensure_pool_open_adaptive()

def _q(query: str, params: tuple | None = None):
    """
    Safe query runner with retries.
    - Attempts up to 3 times.
    - Soft reopen first, full rebuild second, else raise.
    """
    params = params or ()
    exc: Optional[Exception] = None
    for attempt in (1, 2, 3):
        try:
            _ensure_pool_open_adaptive()
            with POOL.connection() as conn:
                _warm_conn(conn)
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(query, params)
                    return cur.fetchall() if cur.description else []
        except (psycopg.OperationalError, psycopg.InterfaceError, PoolTimeout, PoolClosed) as e:
            exc = e
            logger.warning("DB op failed (%s). Attempt %d/3", e.__class__.__name__, attempt)
            if attempt == 1:
                _soft_reopen_pool(); time.sleep(0.2)
            elif attempt == 2:
                _rebuild_pool()
                try:
                    POOL.open()
                except Exception:
                    pass
                time.sleep(0.35)
            else:
                break
    raise exc if exc else RuntimeError("DB query failed unexpectedly")

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

# ── Airtable helpers (catalogue size, excluding External distributor) ─────────
def _airtable_enabled() -> bool:
    return bool(AIRTABLE_API_KEY and AIRTABLE_BASE_ID)

_airtable_cache: Dict[str, Any] = {
    "at": None, "labels": None, "values": None,
    "min_date": None, "max_date": None, "count": 0,
    "ok": False, "error": None
}

def _http_get_json(url: str, headers: Dict[str, str], params: Dict[str, Any], timeout: float = 30.0) -> Dict[str, Any]:
    # Support fields[] arrays
    query = urllib.parse.urlencode(
        [(k, v) for k, vv in params.items() for v in (vv if isinstance(vv, list) else [vv])]
    )
    full = f"{url}?{query}" if query else url
    req = urllib.request.Request(full, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

def _fetch_catalogue_cumulative():
    """Return (labels, values, min_date, max_date, total_count, ok, error)"""
    now = datetime.utcnow()
    # serve cached if fresh
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

    # Only records with a Release Date and Distributor != 'External'
    # Note: blanks are included (NOT(field = 'External')) evaluates true if field blank.
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
                # Accept date-only or ISO datetime
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
<script>
  // Read CSS variables so JS (Chart.js) matches the OS theme
  const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  function applyChartTheme(){
    const fg      = css('--fg') || '#111';
    const line    = css('--line') || 'rgba(0,0,0,.1)';
    const accent  = css('--accent') || '#2563eb';

    Chart.defaults.color = fg;
    Chart.defaults.borderColor = line;
    Chart.defaults.plugins.legend.labels.color = fg;
    Chart.defaults.plugins.tooltip.titleColor = fg;
    Chart.defaults.plugins.tooltip.bodyColor = fg;
    Chart.defaults.plugins.tooltip.backgroundColor = 'rgba(0,0,0,.72)';

    // sensible defaults for all charts
    Chart.defaults.elements.point.radius = 3;
    Chart.defaults.elements.line.tension = 0.3;

    // Keep a global accent we can reuse
    window.__ACCENT_COLOR__ = accent;
  }
  applyChartTheme();

  // Live-update when macOS switches light/dark
  const mq = window.matchMedia('(prefers-color-scheme: dark)');
  mq.addEventListener?.('change', () => { applyChartTheme(); /* charts get recreated in your loaders */ });
</script>

<script src="https://cdn.tailwindcss.com"></script>
<link rel="stylesheet" href="/static/styles.css" />
<style>
  /* Theme tokens */
  :root{
    --bg: #ffffff;
    --fg: #111111;
    --card: #ffffff;
    --muted: rgba(0,0,0,.55);
    --line: rgba(0,0,0,.08);
    --accent: #2563eb;   /* chart line & links */
    --chip: #111111;
    --input-bg:#ffffff;
    --input-border:#d1d5db;
  }
  @media (prefers-color-scheme: dark){
    :root{
      --bg:#0f1115;
      --fg:#f3f4f6;
      --card:rgba(24,24,27,.86);
      --muted: rgba(255,255,255,.65);
      --line: rgba(255,255,255,.12);
      --accent:#60a5fa;
      --chip:#f3f4f6;
      --input-bg:#0d0f13;
      --input-border:#2a2f3a;
    }
  }

  /* Layout & typography */
  html, body { height: 100%; }
  body {
    margin:0;
    background: var(--bg);
    color: var(--fg);
    font-family: -apple-system, system-ui, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji","Segoe UI Emoji";
    -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale;
  }
  .content { padding: 24px; }
  .brand-header { display:flex; align-items:center; justify-content:space-between; margin-bottom:.75rem; }
  .brand-title { font-weight:800; letter-spacing:-0.02em; }

  .card {
    background: var(--card);
    color: var(--fg);
    border-radius:16px;
    box-shadow: 0 10px 28px rgba(0,0,0,.08);
    padding:1.1rem;
    border:1px solid var(--line);
  }
  h2 { font-size:1.125rem; font-weight:600; }

  /* Tables */
  table { width:100%; border-collapse:collapse; }
  th, td { padding:.5rem; border-bottom:1px solid var(--line); }
  th { color: var(--muted); font-weight:600; }

  /* Inputs */
  select, button {
    background: var(--input-bg);
    color: var(--fg);
    border:1px solid var(--input-border);
    border-radius:8px;
  }
  button { cursor:pointer; }

  a { color: var(--accent); }

  .scroll { max-height:420px; overflow:auto; }

  /* Remove the heavy custom font */
  .kaizen-bold { font-family: inherit; font-weight:700; }

  /* Subtle fixed frame */
  .fixed-frame {
    position:fixed; inset:0; pointer-events:none;
    border:18px solid #000;
    opacity:.9; box-sizing:border-box; z-index:9999;
    mix-blend-mode:normal;
  }

  /* Mobile landscape */
  @media (max-width:900px) and (orientation:landscape){
    .content { padding:12px; }
    .card { padding:.85rem; border-radius:12px; }
    h2 { font-size:1rem; }
    .brand-title { font-size:1rem; }
    canvas { max-height:240px; }
  }
</style>

</head>
<body>
  <div class="fixed-frame"></div>

  <div class="content max-w-7xl mx-auto">
    <header class="brand-header">
      <div class="brand-title text-xl sm:text-2xl">Catalogue Dashboard</div>
      <!-- removed logo and timezone -->
    </header>

    <!-- Row 1: Daily Streams full width -->
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

    <!-- Row 2: Playlists selector (left) + Playlist Growth (right) -->
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

    <!-- Row 3: Catalogue size over time (Airtable, excluding External) -->
    <section class="grid grid-cols-1 gap-6 mt-6">
      <div class="card">
        <div class="flex items-center justify-between mb-3">
          <h2>Catalogue Size Over Time</h2>
          <div class="text-sm opacity-70" id="catalogueMeta"></div>
        </div>
        <canvas id="catalogueChart" height="120"></canvas>
      </div>
    </section>

    <!-- Row 4: Best artists today -->
    <section class="grid grid-cols-1 gap-6 mt-6">
      <div class="card">
        <div class="flex items-center justify-between mb-3">
          <h2>Best Artists Today — Share of Total Streams Δ</h2>
          <div class="text-sm opacity-70" id="bestArtistsDateLabel"></div>
        </div>
        <canvas id="bestArtistsChart" height="120"></canvas>
      </div>
    </section>

    <!-- Row 5: Table -->
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
  let streamsChart, playlistChart, bestArtistsChart, catalogueChart;
  const fmt = (n) => Number(n).toLocaleString();
  async function api(path) { const r = await fetch(path); if (!r.ok) throw new Error(await r.text()); return r.json(); }

  // Streams (LINE chart)
  async function loadStreams(days) {
    const data = await api('/api/streams/total-daily?days=' + days);
    const ctx = document.getElementById('streamsChart').getContext('2d');
    const cfg = {
      type: 'line',
      data: { labels: data.labels, datasets: [{
        label: 'Streams Δ (sum)',
        data: data.values,
        tension: 0.3,
        fill: false
      }]},
      options: {
        responsive: true, maintainAspectRatio: true,
        scales: { x: { ticks: { maxRotation: 0, autoSkip: true } }, y: { beginAtZero: true } },
        plugins: { tooltip: { callbacks: { label: (c) => ' ' + fmt(c.parsed.y) } } }
      }
    };
    if (streamsChart) streamsChart.destroy(); streamsChart = new Chart(ctx, cfg);
  }

  // Playlists list
  async function loadPlaylists() {
    const list = await api('/api/playlists/list');
    const sel = document.getElementById('playlistSelect'); sel.innerHTML = '';
    let defaultId = list.length ? list[0].playlist_id : null;
    for (const p of list) {
      const opt = document.createElement('option');
      opt.value = p.playlist_id;
      opt.textContent = p.playlist_name || p.playlist_id;
      sel.appendChild(opt);
    }
    const def = list.find(p => (p.playlist_name || '').toLowerCase().startsWith(DEFAULT_PLAYLIST_NAME.toLowerCase()));
    if (def) defaultId = def.playlist_id;
    if (defaultId) sel.value = defaultId;
    await updatePlaylistCard();
    await loadPlaylistChart(document.getElementById('playlistDays').value);
  }

  async function updatePlaylistCard() {
    const list = await api('/api/playlists/list');
    const id = document.getElementById('playlistSelect').value;
    const p = list.find(x => x.playlist_id === id); if (!p) return;
    document.getElementById('plFollowers').textContent = fmt(p.followers ?? 0);
    document.getElementById('plDelta').textContent = (p.delta == null) ? '-' : fmt(p.delta);
    document.getElementById('plLink').href = p.web_url;
  }

  // Playlist Growth
  async function loadPlaylistChart(days) {
    const id = document.getElementById('playlistSelect').value; if (!id) return;
    const data = await api('/api/playlists/' + encodeURIComponent(id) + '/series?days=' + days);
    const ctx = document.getElementById('playlistChart').getContext('2d');
    const cfg = {
      data: { labels: data.labels, datasets: [
        { type: 'line', label: 'Followers', data: data.followers, yAxisID: 'y1', tension: 0.25 },
        { type: 'bar',  label: 'Daily Δ',  data: data.deltas,    yAxisID: 'y2' }
      ]},
      options: {
        responsive: true, maintainAspectRatio: true,
        scales: {
          x: { ticks: { maxRotation: 0, autoSkip: true } },
          y1: { type: 'linear', position: 'left', beginAtZero: true },
          y2: { type: 'linear', position: 'right', beginAtZero: true, grid: { drawOnChartArea: false } }
        },
        plugins: { tooltip: { callbacks: { label: (c) => ' ' + fmt(c.parsed.y) } } }
      }
    };
    if (playlistChart) playlistChart.destroy(); playlistChart = new Chart(ctx, cfg);
    await updatePlaylistCard();
  }

  // Best artists today
  async function loadBestArtists() {
    const data = await api('/api/artists/top-share');
    document.getElementById('bestArtistsDateLabel').textContent = 'Date: ' + data.date;
    const ctx = document.getElementById('bestArtistsChart').getContext('2d');
    if (bestArtistsChart) bestArtistsChart.destroy();
    bestArtistsChart = new Chart(ctx, {
      type: 'bar',
      data: { labels: data.labels, datasets: [{
        label: 'Share of Daily Streams Δ (%)',
        data: data.shares
      }]},
      options: {
        responsive: true, maintainAspectRatio: true,
        plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => ' ' + c.parsed.y.toFixed(2) + '%' } } },
        scales: { y: { beginAtZero: true, ticks: { callback: (v)=> v + '%' } } }
      }
    });
  }

  // Catalogue size
  async function loadCatalogue() {
    const data = await api('/api/catalogue/size-series');
    const ctx = document.getElementById('catalogueChart').getContext('2d');
    if (catalogueChart) catalogueChart.destroy();
    catalogueChart = new Chart(ctx, {
      type: 'line',
      data: { labels: data.labels, datasets: [{
        label: 'Total tracks in catalogue',
        data: data.values,
        tension: 0.25,
        fill: false
      }]},
      options: {
        responsive: true, maintainAspectRatio: true,
        scales: { x: { ticks: { maxRotation: 0, autoSkip: true } }, y: { beginAtZero: true } },
        plugins: { tooltip: { callbacks: { label: (c) => ' ' + fmt(c.parsed.y) } } }
      }
    });
    const meta = document.getElementById('catalogueMeta');
    const total = (data.values && data.values.length) ? data.values[data.values.length-1] : 0;
    meta.textContent = `Total: ${fmt(total)} • Range: ${data.min_date ?? '-'} → ${data.max_date ?? '-'} • Counted: ${fmt(data.count ?? 0)} (excl. 'External')`;
  }

  // Dates + table
  async function loadDeltaDates(days) {
    const data = await api('/api/streams/dates?days=' + days);
    const sel = document.getElementById('deltaDate'); sel.innerHTML = '';
    for (const d of data.dates) {
      const opt = document.createElement('option'); opt.value = d; opt.textContent = d; sel.appendChild(opt);
    }
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

  // Listeners
  document.getElementById('streamsDays').addEventListener('change', e => loadStreams(e.target.value));
  document.getElementById('playlistDays').addEventListener('change', e => loadPlaylistChart(e.target.value));
  document.getElementById('playlistSelect').addEventListener('change', async () => {
    await updatePlaylistCard(); await loadPlaylistChart(document.getElementById('playlistDays').value);
  });
  document.getElementById('btnReloadDeltas').addEventListener('click', loadDeltaTable);

  // Bootstrap
  (async () => {
    await loadStreams(document.getElementById('streamsDays').value);
    await loadPlaylists();
    await loadPlaylistChart(document.getElementById('playlistDays').value);
    await loadCatalogue();
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
    q = """
        SELECT s.stream_date AS d,
               COALESCE(SUM(GREATEST(s.daily_delta, 0)), 0)::bigint AS v
        FROM streams s
        WHERE s.platform = 'spotify'
          AND s.stream_date >= CURRENT_DATE - %s::int * INTERVAL '1 day'
        GROUP BY s.stream_date
        ORDER BY s.stream_date
    """
    rows = _q(q, (days,))
    if rows:
        start, end = rows[0]["d"], rows[-1]["d"]
    else:
        end = date.today(); start = end - timedelta(days=days)
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

    # Return normalized artist in the table (first credited name)
    q = r"""
        SELECT
          t.isrc,
          t.title,
          (regexp_split_to_array(
             btrim(replace(t.artist, E'\u00D7', ' x ')),
             '(?i)\s*(?:,|&|;|\+|x|×|with|feat\.?|featuring)\s*'
           ))[1] AS artist_norm,
          COALESCE(s.daily_delta, 0)::bigint AS delta
        FROM streams s
        JOIN track_dim t ON t.track_uid = s.track_uid
        WHERE s.platform = 'spotify'
          AND s.stream_date = %s
        ORDER BY COALESCE(s.daily_delta,0) DESC, t.isrc
        LIMIT %s
    """
    rows = _q(q, (day, limit))
    out = []
    for r in rows:
        out.append({
            "isrc": r.get("isrc"),
            "title": r.get("title"),
            "artist": r.get("artist_norm"),  # show normalized
            "delta": int(r.get("delta") or 0),
        })
    return jsonify({"rows": out})


# ── API: playlists (list + single series) ─────────────────────────────────────
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

# (Endpoint kept though no longer used in UI)
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

# Best artists for the latest date — share of daily delta
@app.get("/api/artists/top-share")
def api_artists_top_share():
    # latest date
    q_latest = "SELECT MAX(stream_date) AS d FROM streams WHERE platform='spotify'"
    r = _q(q_latest)
    latest = r[0]["d"] if r and r[0]["d"] else None
    day = request.args.get("date") or (latest.isoformat() if latest else None)
    if not day:
        return jsonify({"date": None, "labels": [], "values": [], "shares": []})

    # Normalize artist by taking the first credited name
    # separators handled: comma, ampersand, semicolon, plus, x/×, with, feat./featuring
    q = r"""
        WITH base AS (
          SELECT
            (regexp_split_to_array(
               btrim(replace(t.artist, E'\u00D7', ' x ')),
               '(?i)\s*(?:,|&|;|\+|x|×|with|feat\.?|featuring)\s*'
             ))[1] AS artist_norm,
            GREATEST(s.daily_delta, 0)::bigint AS v
          FROM streams s
          JOIN track_dim t ON t.track_uid = s.track_uid
          WHERE s.platform = 'spotify'
            AND s.stream_date = %s
        )
        SELECT artist_norm AS artist,
               COALESCE(SUM(v),0)::bigint AS v
        FROM base
        GROUP BY artist_norm
        HAVING COALESCE(SUM(v),0) > 0
        ORDER BY v DESC
        LIMIT 25
    """
    rows = _q(q, (day,))
    total = sum(int(r["v"]) for r in rows) or 1

    labels = [(r["artist"] or "") for r in rows]
    values = [int(r["v"]) for r in rows]
    shares = [round(v * 100.0 / total, 2) for v in values]
    return jsonify({"date": day, "labels": labels, "values": values, "shares": shares})

# Catalogue size series API (Airtable, distributor != 'External')
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
