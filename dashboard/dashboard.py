# dashboard.py — KAIZEN-styled Flask dashboard (Streams, Playlist Growth, Cumulative Streams + Catalogue Size)
import os
from datetime import date, timedelta
from typing import List, Tuple

from flask import Flask, jsonify, render_template_string, request
from psycopg_pool import ConnectionPool, PoolClosed
from psycopg.rows import dict_row

# ── Config ────────────────────────────────────────────────────────────────────
LOCAL_TZ = os.getenv("LOCAL_TZ", "Europe/London")
DEFAULT_PLAYLIST_NAME = os.getenv("DEFAULT_PLAYLIST_NAME", "TOGI Motivation")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

POOL = ConnectionPool(
    conninfo=DATABASE_URL,
    kwargs={"sslmode": os.getenv("DB_SSLMODE", "prefer")},
    min_size=1,
    max_size=5,
    open=False,
)

app = Flask(__name__)  # serves /static/* by default


# ── DB helpers ────────────────────────────────────────────────────────────────
def _ensure_pool_open():
    try:
        POOL.open()
    except Exception:
        pass

def _q(query: str, params: tuple | None = None):
    _ensure_pool_open()
    try:
        with POOL.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params or ())
            return cur.fetchall()
    except PoolClosed:
        _ensure_pool_open()
        with POOL.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params or ())
            return cur.fetchall()


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
    .fixed-frame { position: fixed; inset: 0; pointer-events: none; border: 18px solid #000; box-sizing: border-box; z-index: 9999; }
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
    .kaizen-bold { font-family: "THE BOLD FONT - FREE VERSION - 2023", ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, "Helvetica Neue", Arial; }
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
        <div class="flex items-center justify-between mb-3">
          <h2>Cumulative Streams (Spotify)</h2>
          <div>
            <label class="mr-2 text-sm opacity-70">Window</label>
            <select id="cumulativeDays" class="border rounded px-2 py-1 text-sm">
              <option value="30">30 days</option>
              <option value="90" selected>90 days</option>
              <option value="180">180 days</option>
            </select>
          </div>
        </div>
        <canvas id="cumulativeChart" height="110"></canvas>
      </div>
    </section>

    <!-- Row 2 -->
    <section class="grid grid-cols-1 lg:grid-cols-3 gap-6 mt-6">
      <div class="card">
        <div class="flex items-center justify-between mb-3">
          <h2>Playlist Followers ({{ default_playlist_name }})</h2>
          <div>
            <label class="mr-2 text-sm opacity-70">Window</label>
            <select id="playlistDays" class="border rounded px-2 py-1 text-sm">
              <option value="30">30 days</option>
              <option value="90" selected>90 days</option>
              <option value="180">180 days</option>
            </select>
          </div>
        </div>
        <canvas id="playlistChart" height="110"></canvas>
      </div>

      <!-- New Catalogue Chart -->
      <div class="card col-span-1 lg:col-span-2">
        <div class="flex items-center justify-between mb-3">
          <h2>Catalogue Size Over Time</h2>
          <div>
            <label class="mr-2 text-sm opacity-70">Window</label>
            <select id="catalogueDays" class="border rounded px-2 py-1 text-sm">
              <option value="90">90 days</option>
              <option value="180">180 days</option>
              <option value="365" selected>365 days</option>
            </select>
          </div>
        </div>
        <canvas id="catalogueChart" height="110"></canvas>
      </div>
    </section>

    <!-- Rest unchanged (tables, best artists, etc.) -->
  </div>

<script>
  const DEFAULT_PLAYLIST_NAME = {{ default_playlist_name | tojson }};
  let streamsChart, cumulativeChart, playlistChart, catalogueChart;

  const fmt = (n) => Number(n).toLocaleString();
  async function api(path) { const r = await fetch(path); if (!r.ok) throw new Error(await r.text()); return r.json(); }

  async function loadStreams(days) {
    const data = await api('/api/streams/total-daily?days=' + days);
    const ctx = document.getElementById('streamsChart').getContext('2d');
    if (streamsChart) streamsChart.destroy();
    streamsChart = new Chart(ctx, {
      type: 'line',
      data: { labels: data.labels, datasets: [{ label: 'Streams Δ (sum)', data: data.values, tension: 0.3 }] },
      options: { responsive: true, scales: { y: { beginAtZero: true } } }
    });
  }

  async function loadCumulative(days) {
    const data = await api('/api/streams/total-cumulative?days=' + days);
    const ctx = document.getElementById('cumulativeChart').getContext('2d');
    if (cumulativeChart) cumulativeChart.destroy();
    cumulativeChart = new Chart(ctx, {
      type: 'line',
      data: { labels: data.labels, datasets: [{ label: 'Cumulative Streams', data: data.values, borderColor: '#ef4444', tension: 0.3 }] },
      options: { responsive: true, scales: { y: { beginAtZero: true } } }
    });
  }

  async function loadPlaylist(days) {
    const data = await api('/api/playlists/one?days=' + days + '&name=' + encodeURIComponent(DEFAULT_PLAYLIST_NAME));
    const ctx = document.getElementById('playlistChart').getContext('2d');
    if (playlistChart) playlistChart.destroy();
    playlistChart = new Chart(ctx, {
      type: 'line',
      data: { labels: data.labels, datasets: [{ label: 'Followers', data: data.values, borderColor: '#22c55e', tension: 0.3 }] },
      options: { responsive: true, scales: { y: { beginAtZero: true } } }
    });
  }

  async function loadCatalogue(days) {
    const data = await api('/api/catalogue/size-over-time?days=' + days);
    const ctx = document.getElementById('catalogueChart').getContext('2d');
    if (catalogueChart) catalogueChart.destroy();
    catalogueChart = new Chart(ctx, {
      type: 'line',
      data: { labels: data.labels, datasets: [{ label: 'Total Releases', data: data.values, borderColor: '#3b82f6', tension: 0.3 }] },
      options: { responsive: true, scales: { y: { beginAtZero: true } } }
    });
  }

  document.getElementById('streamsDays').addEventListener('change', e => loadStreams(e.target.value));
  document.getElementById('cumulativeDays').addEventListener('change', e => loadCumulative(e.target.value));
  document.getElementById('playlistDays').addEventListener('change', e => loadPlaylist(e.target.value));
  document.getElementById('catalogueDays').addEventListener('change', e => loadCatalogue(e.target.value));

  (async () => {
    await loadStreams(document.getElementById('streamsDays').value);
    await loadCumulative(document.getElementById('cumulativeDays').value);
    await loadPlaylist(document.getElementById('playlistDays').value);
    await loadCatalogue(document.getElementById('catalogueDays').value);
  })();
</script>
</body>
</html>"""
    return render_template_string(html, local_tz=LOCAL_TZ, default_playlist_name=DEFAULT_PLAYLIST_NAME)


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


@app.get("/api/streams/total-cumulative")
def api_streams_total_cumulative():
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
    running = 0
    series = []
    for r in rows:
        running += int(r["v"])
        series.append((r["d"], running))
    if series:
        start, end = series[0][0], series[-1][0]
    else:
        end = date.today(); start = end - timedelta(days=days)
    labels, values = _fill_series(series, start, end)
    return jsonify({"labels": labels, "values": values})


# ── API: catalogue ────────────────────────────────────────────────────────────
@app.get("/api/catalogue/size-over-time")
def api_catalogue_size_over_time():
    days = _clamp_days(request.args.get("days"), 365)
    q = """
        SELECT c.release_date::date AS d,
               COUNT(*) AS v
        FROM catalogue c
        WHERE c.release_date IS NOT NULL
          AND c.release_date >= CURRENT_DATE - %s::int * INTERVAL '1 day'
        GROUP BY c.release_date::date
        ORDER BY c.release_date::date
    """
    rows = _q(q, (days,))
    running = 0
    series = []
    for r in rows:
        running += int(r["v"])
        series.append((r["d"], running))
    if series:
        start, end = series[0][0], series[-1][0]
    else:
        end = date.today(); start = end - timedelta(days=days)
    labels, values = _fill_series(series, start, end)
    return jsonify({"labels": labels, "values": values})


# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    try:
        _q("SELECT NOW()")
        ssl = _q("SHOW ssl")
        return {"ok": True, "db": True, "ssl": (ssl[0]["ssl"] if ssl else "unknown")}
    except Exception as e:
        return {"ok": False, "db": False, "error": str(e)}


# ── Entrypoint ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, threaded=True)
