# dashboard.py — KAIZEN-styled Flask dashboard (Streams & Playlist Growth + Catalogue)
import os
import time
import logging
from datetime import date, datetime, timedelta
from typing import List, Tuple, Dict, Any
from collections import defaultdict
import json
import urllib.parse
import urllib.request

from flask import Flask, jsonify, render_template_string, request
import psycopg
from psycopg_pool import ConnectionPool, PoolClosed
from psycopg.rows import dict_row

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

# ── DB pool (resilient against restarts/idle timeouts) ────────────────────────
# Enforce SSL and enable TCP keepalives so dead peers are detected promptly.
conn_kwargs = {
    "sslmode": "require",          # was "prefer" → make it strict
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 5,
}

POOL = ConnectionPool(
    conninfo=DATABASE_URL,
    kwargs=conn_kwargs,
    min_size=1,
    max_size=int(os.getenv("DB_POOL_MAX", "5")),
    timeout=10,         # wait up to 10s for a free conn
    max_lifetime=3600,  # recycle hourly to avoid stale sockets
    max_idle=300,       # recycle idle conns after 5m
    open=False,         # lazy-open
)

app = Flask(__name__)  # serves /static/* by default

# ── DB helpers ────────────────────────────────────────────────────────────────
def _ensure_pool_open():
    try:
        POOL.open()
    except Exception:
        # Pool may already be open; ignore
        pass

def _reopen_pool():
    try:
        POOL.close()
    except Exception:
        pass
    time.sleep(0.25)
    POOL.open()

def _q(query: str, params: tuple | None = None):
    """
    Execute a read-only query with one automatic reconnect retry if the server
    closed the connection (restart, network flap, idle timeout).
    Returns list of dict rows.
    """
    params = params or ()
    for attempt in (1, 2):
        try:
            _ensure_pool_open()
            with POOL.connection() as conn:
                # cheap ping so we fail fast on a dead socket
                with conn.cursor() as ping:
                    ping.execute("SELECT 1")
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(query, params)
                    if cur.description:
                        return cur.fetchall()
                    return []
        except (psycopg.OperationalError, psycopg.InterfaceError) as e:
            logging.warning("DB op failed (%s). Attempt %d/2", e.__class__.__name__, attempt)
            if attempt == 2:
                raise
            _reopen_pool()
        except PoolClosed:
            # Very rare: pool got closed—reopen and retry.
            if attempt == 2:
                raise
            _reopen_pool()

def _exec(query: str, params: tuple | None = None) -> int:
    """
    Execute a write/DDL with one automatic reconnect retry.
    Returns affected rowcount.
    """
    params = params or ()
    for attempt in (1, 2):
        try:
            _ensure_pool_open()
            with POOL.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    conn.commit()
                    return cur.rowcount
        except (psycopg.OperationalError, psycopg.InterfaceError) as e:
            logging.warning("DB exec failed (%s). Attempt %d/2", e.__class__.__name__, attempt)
            if attempt == 2:
                raise
            _reopen_pool()
        except PoolClosed:
            if attempt == 2:
                raise
            _reopen_pool()


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
    seen_min: date | None = None
    seen_max: date | None = None

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
  <title>KAIZEN — Catalogue Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="stylesheet" href="/static/styles.css" />
  <style>
    /* Fixed black frame that always shows during scroll */
    .fixed-frame {
      position: fixed; inset: 0;
      pointer-events: none;
      border: 18px solid #000;
      box-sizing: border-box;
      z-index: 9999;
    }
    html, body { height: 100%; }
    body { margin: 0; background: #fff; color: #111; }
    .content { padding: 24px; }
    .brand-header { display:flex; align-items:center; gap:.85rem; margin-bottom: .75rem; }
    .brand-logo { height: 64px; width: 64px; object-fit: contain; }
    .brand-title { font-weight: 800; letter-spacing: -0.02em; }
    .card { background: #fff; border-radius: 16px; box-shadow: 0 10px 28px rgba(0,0,0,.08); padding: 1.1rem; }
    h2 { font-size: 1.125rem; font-weight: 600; }
    table { width:100%; border-collapse: collapse; }
    th, td { padding:.5rem; border-bottom:1px solid rgba(0,0,0,.06); }
    .scroll { max-height: 420px; overflow:auto; }
    .kaizen-bold { font-family: "THE BOLD FONT - FREE VERSION - 2023", ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, "Helvetica Neue", Arial, "Noto Sans", "Apple Color Emoji","Segoe UI Emoji"; }
    @media (prefers-color-scheme: dark) {
      body { background:#111; color:#f5f5f5; }
      .card { background: rgba(24,24,27,.82); color:#fff; }
      th, td { border-color: rgba(255,255,255,.08); }
      a { color: #f87171; }
    }
  </style>
</head>
<body>
  <div class="fixed-frame"></div>

  <div class="content max-w-7xl mx-auto">
    <header class="brand-header">
      <img class="brand-logo" src="/static/img/kaizen_ring_red.png" alt="KAIZEN" />
      <div>
        <div class="brand-title text-xl sm:text-2xl">Catalogue Dashboard</div>
        <div class="text-xs opacity-70">Timezone: {{ local_tz }}</div>
      </div>
    </header>

    <!-- Row 1 -->
    <section class="grid grid-cols-1 lg:grid-cols-3 gap-6">
      <div class="card col-span-1 lg:col-span-2">
        <div class="flex items-center justify-between mb-3">
          <h2>Daily Streams Δ (sum)</h2>
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
    </section>

    <!-- Row 2 -->
    <section class="grid grid-cols-1 lg:grid-cols-2 gap-6 mt-6">
      <div class="card">
        <div class="flex items-center justify-between mb-3">
          <h2>Playlist Growth (followers & daily Δ)</h2>
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

      <div class="card">
        <div class="flex items-center justify-between mb-3">
          <h2>All Playlists — Followers Over Time</h2>
          <div>
            <label class="mr-2 text-sm opacity-70">Window</label>
            <select id="allPlaylistsDays" class="border rounded px-2 py-1 text-sm">
              <option value="30">30 days</option>
              <option value="90" selected>90 days</option>
              <option value="180">180 days</option>
            </select>
          </div>
        </div>
        <canvas id="allPlaylistsChart" height="120"></canvas>
      </div>
    </section>

    <!-- NEW Row 3: Catalogue size over time (Airtable, excluding External) -->
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
          <h2>Top track deltas by day</h2>
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
  let streamsChart, playlistChart, allPlaylistsChart, bestArtistsChart, catalogueChart;
  const fmt = (n) => Number(n).toLocaleString();
  async function api(path) { const r = await fetch(path); if (!r.ok) throw new Error(await r.text()); return r.json(); }

  // Streams (LINE chart)
  async function loadStreams(days) {
    const data = await api('/api/streams/total-daily?days=' + days);
    const ctx = document.getElementById('streamsChart').getContext('2d');
    const cfg = {
      type: 'line',
      data: { labels: data.labels, datasets: [{ label: 'Streams Δ (sum)', data: data.values, tension: 0.3, fill: false }] },
      options: { responsive: true, scales: { x: { ticks: { maxRotation: 0, autoSkip: true } }, y: { beginAtZero: true } },
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
    const a = document.getElementById('plLink'); a.href = p.web_url;
  }

  // Playlist Growth (selected playlist)
  async function loadPlaylistChart(days) {
    const id = document.getElementById('playlistSelect').value; if (!id) return;
    const data = await api('/api/playlists/' + encodeURIComponent(id) + '/series?days=' + days);
    const ctx = document.getElementById('playlistChart').getContext('2d');
    const cfg = {
      data: { labels: data.labels, datasets: [
        { type: 'line', label: 'Followers', data: data.followers, yAxisID: 'y1', tension: 0.25 },
        { type: 'bar',  label: 'Daily Δ',  data: data.deltas,    yAxisID: 'y2' }
      ]},
      options: { responsive: true, scales: {
        x: { ticks: { maxRotation: 0, autoSkip: true } },
        y1: { type: 'linear', position: 'left', beginAtZero: true },
        y2: { type: 'linear', position: 'right', beginAtZero: true, grid: { drawOnChartArea: false } }
      }, plugins: { tooltip: { callbacks: { label: (c) => ' ' + fmt(c.parsed.y) } } } }
    };
    if (playlistChart) playlistChart.destroy(); playlistChart = new Chart(ctx, cfg); await updatePlaylistCard();
  }

  // All playlists followers (multi-line)
  async function loadAllPlaylistsChart(days) {
    const data = await api('/api/playlists/all-series?days=' + days);
    const ctx = document.getElementById('allPlaylistsChart').getContext('2d');
    const datasets = data.series.map(s => ({
      type: 'line', label: s.name, data: s.values, tension: 0.25, fill: false
    }));
    if (allPlaylistsChart) allPlaylistsChart.destroy();
    allPlaylistsChart = new Chart(ctx, {
      data: { labels: data.labels, datasets },
      options: { responsive: true, plugins: { legend: { position: 'bottom' }, tooltip: { callbacks: { label: (c) => ' ' + fmt(c.parsed.y) } } },
                 scales: { x: { ticks: { maxRotation: 0, autoSkip: true } }, y: { beginAtZero: true } } }
    });
  }

  // Best artists today (share of total streams delta)
  async function loadBestArtists() {
    const data = await api('/api/artists/top-share');
    document.getElementById('bestArtistsDateLabel').textContent = 'Date: ' + data.date;
    const ctx = document.getElementById('bestArtistsChart').getContext('2d');
    if (bestArtistsChart) bestArtistsChart.destroy();
    bestArtistsChart = new Chart(ctx, {
      type: 'bar',
      data: { labels: data.labels, datasets: [{ label: 'Share of Daily Streams Δ (%)', data: data.shares }] },
      options: { responsive: true, plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => ' ' + c.parsed.y.toFixed(2) + '%' } } },
                 scales: { y: { beginAtZero: true, ticks: { callback: (v)=> v + '%' } } } }
    });
  }

  // NEW: Catalogue size (Airtable, excluding "External")
  async function loadCatalogue() {
    const data = await api('/api/catalogue/size-series');
    const ctx = document.getElementById('catalogueChart').getContext('2d');
    if (catalogueChart) catalogueChart.destroy();
    catalogueChart = new Chart(ctx, {
      type: 'line',
      data: { labels: data.labels, datasets: [{ label: 'Total tracks in catalogue', data: data.values, tension: 0.25, fill: false }] },
      options: { responsive: true, scales: { x: { ticks: { maxRotation: 0, autoSkip: true } }, y: { beginAtZero: true } },
                 plugins: { tooltip: { callbacks: { label: (c) => ' ' + fmt(c.parsed.y) } } } }
    });
    const meta = document.getElementById('catalogueMeta');
    const total = (data.values && data.values.length) ? data.values[data.values.length-1] : 0;
    meta.textContent = `Total: ${fmt(total)} • Range: ${data.min_date ?? '-'} → ${data.max_date ?? '-'} • Counted: ${fmt(data.count ?? 0)} (excl. 'External')`;
  }

  document.getElementById('streamsDays').addEventListener('change', e => loadStreams(e.target.value));
  document.getElementById('playlistDays').addEventListener('change', e => loadPlaylistChart(e.target.value));
  document.getElementById('allPlaylistsDays').addEventListener('change', e => loadAllPlaylistsChart(e.target.value));
  document.getElementById('playlistSelect').addEventListener('change', async () => { await updatePlaylistCard(); await loadPlaylistChart(document.getElementById('playlistDays').value); });
  document.getElementById('btnReloadDeltas').addEventListener('click', loadDeltaTable);

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

  (async () => {
    await loadStreams(document.getElementById('streamsDays').value);
    await loadPlaylists();
    await loadPlaylistChart(document.getElementById('playlistDays').value);
    await loadAllPlaylistsChart(document.getElementById('allPlaylistsDays').value);
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
    q = """
        SELECT
          t.isrc,
          t.title,
          t.artist,
          COALESCE(s.daily_delta, 0)::bigint AS delta
        FROM streams s
        JOIN track_dim t ON t.track_uid = s.track_uid
        WHERE s.platform='spotify'
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
            "artist": r.get("artist"),
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


# NEW: All playlists — followers over time (multi-series)
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

    series_map = {}
    for r in rows:
        pid = r["playlist_id"]
        series_map.setdefault(pid, {"name": r["name"], "points": []})
        series_map[pid]["points"].append((r["d"], int(r["followers"] or 0)))

    series = []
    for obj in series_map.values():
        _, vals = _fill_series(obj["points"], start, end)
        series.append({"name": obj["name"], "values": vals})

    return jsonify({"labels": labels_base, "series": series})


# NEW: Best artists for the latest date — share of daily delta
@app.get("/api/artists/top-share")
def api_artists_top_share():
    q_latest = """
        SELECT MAX(stream_date) AS d
        FROM streams
        WHERE platform='spotify'
    """
    r = _q(q_latest)
    latest = r[0]["d"] if r and r[0]["d"] else None
    day = request.args.get("date") or (latest.isoformat() if latest else None)
    if not day:
        return jsonify({"date": None, "labels": [], "values": [], "shares": []})

    q = """
        SELECT t.artist,
               COALESCE(SUM(GREATEST(s.daily_delta, 0)),0)::bigint AS v
        FROM streams s
        JOIN track_dim t ON t.track_uid = s.track_uid
        WHERE s.platform='spotify'
          AND s.stream_date = %s
        GROUP BY t.artist
        HAVING COALESCE(SUM(GREATEST(s.daily_delta, 0)),0) > 0
        ORDER BY v DESC
        LIMIT 25
    """
    rows = _q(q, (day,))
    total = sum(int(r["v"]) for r in rows) or 1

    labels = [(r["artist"] or "") for r in rows]
    values = [int(r["v"]) for r in rows]
    shares = [round(v * 100.0 / total, 2) for v in values]
    return jsonify({"date": day, "labels": labels, "values": values, "shares": shares})


# NEW: Catalogue size series API (Airtable, distributor != 'External')
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
