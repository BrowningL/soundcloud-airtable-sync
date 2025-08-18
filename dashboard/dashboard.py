# dashboard.py — KAIZEN-styled Flask dashboard (Streams & Playlist Growth)
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

# Railway TCP proxy often needs sslmode=disable (you set DB_SSLMODE=disable)
POOL = ConnectionPool(
    conninfo=DATABASE_URL,
    kwargs={"sslmode": os.getenv("DB_SSLMODE", "prefer")},
    min_size=1,
    max_size=5,
    open=False,  # lazy open; good with gunicorn workers
)

app = Flask(__name__)  # static folder defaults to "./static"


# ── DB helpers ────────────────────────────────────────────────────────────────
def _ensure_pool_open():
    try:
        POOL.open()
    except Exception:
        pass  # already open or racing

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
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>KAIZEN — Streams & Playlists Dashboard</title>

  <!-- Vendors -->
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <script src="https://cdn.tailwindcss.com"></script>

  <!-- Your brand CSS (contains the clock/reveal animation + font-face) -->
  <link rel="stylesheet" href="/static/styles.css" />

  <style>
    /* ===== KAIZEN frame + layout ===== */
    html, body { height: 100%; }
    body { margin: 0; background: #000; color: #111; }

    /* Black border (like the site) */
    .frame {
      position: relative;
      background: #fff;
      margin: 18px;                    /* thickness of the outer black border */
      min-height: calc(100vh - 36px);
      border: 2px solid #000;          /* crisp inner line */
      overflow: hidden;
    }

    /* Header (logo lockup top-left) */
    .brand-header {
      display: flex; align-items: center; gap: .75rem;
      padding: 14px 16px 10px 16px;
    }
    .brand-logo { height: 44px; width: 44px; object-fit: contain; }
    .brand-title { font-weight: 900; letter-spacing: -0.02em; }

    /* Cards */
    .card {
      background: #fff;
      border-radius: 16px;
      box-shadow: 0 10px 28px rgba(0,0,0,.08);
      padding: 1.1rem;
    }

    /* Tables */
    table { width: 100%; border-collapse: collapse; }
    th, td { padding: .5rem; border-bottom: 1px solid rgba(0,0,0,.06); }
    .scroll { max-height: 420px; overflow: auto; }

    /* Dark scheme polish */
    @media (prefers-color-scheme: dark) {
      .frame { background: #111; color: #f6f6f6; }
      .card { background: rgba(24,24,27,.82); color: #f6f6f6; }
      table th, table td { border-color: rgba(255,255,255,.08) !important; }
      a { color: #f87171; }
    }

    /* ===== Preloader (text pops quickly, ring reveals clockwise) =====
       We piggyback on classes defined in /static/styles.css:
       - .clock: conic-gradient mask that sweeps clockwise
       - @font-face for THE BOLD FONT
    */
    .preloader {
      position: absolute; inset: 0;
      display: grid; place-items: center;
      background: inherit;          /* match white or dark of frame */
      z-index: 50;
    }
    .pre-inner { position: relative; width: min(72vmin, 540px); height: min(72vmin, 540px); }
    .pre-enso {
      position: absolute; inset: 0; margin: auto;
      width: 100%; height: 100%; object-fit: contain;
      filter: drop-shadow(0 6px 16px rgba(0,0,0,.18));
    }
    .pre-word {
      position: absolute; inset: 0; display: grid; place-items: center;
      font-family: "THE BOLD FONT - FREE VERSION - 2023", system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      font-weight: 900; letter-spacing: -.03em;
      font-size: clamp(36px, 11vmin, 96px);
      color: #000;
      opacity: 0; transform: translateY(8px);
      animation: wordIn .35s ease-out .20s forwards;  /* text pops quickly first */
      text-shadow: 0 1px 0 rgba(0,0,0,.03);
    }
    @media (prefers-color-scheme: dark) { .pre-word { color: #fff; } }

    @keyframes wordIn {
      to { opacity: 1; transform: translateY(0); }
    }

    /* Reveal app after splash */
    .app { opacity: 0; transform: translateY(6px); transition: opacity .45s ease, transform .45s ease; }
    body.loaded .preloader { opacity: 0; visibility: hidden; transition: opacity .45s ease .12s, visibility 0s linear .6s; }
    body.loaded .app { opacity: 1; transform: none; }

    /* Motion-friendly */
    @media (prefers-reduced-motion: reduce) {
      .pre-word, .clock { animation: none !important; }
    }
  </style>
</head>
<body>
  <div class="frame">

    <!-- PRELOADER: Text appears fast; ring reveals clockwise using .clock mask -->
    <div class="preloader" id="preloader">
      <div class="pre-inner">
        <div class="pre-word">KAIZEN</div>
        <!-- IMPORTANT: use /static/img/kaizen_ring_red.png (rename your file if needed) -->
        <img class="pre-enso clock" src="/static/img/kaizen_ring_red.png" alt="KAIZEN ring" />
      </div>
    </div>

    <!-- Header lockup -->
    <header class="brand-header">
      <!-- Tiny ring serves as lockup; you can replace with a full wordmark PNG if you prefer -->
      <img class="brand-logo" src="/static/img/kaizen_ring_red.png" alt="KAIZEN" />
      <div>
        <div class="brand-title text-xl sm:text-2xl">Streams & Playlists Dashboard</div>
        <div class="text-xs opacity-70">Timezone: {{ local_tz }}</div>
      </div>
    </header>

    <!-- Main app content -->
    <div class="app">
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pb-8">
        <section class="grid grid-cols-1 lg:grid-cols-3 gap-6 mt-2">
          <div class="card col-span-1 lg:col-span-2">
            <div class="flex items-center justify-between mb-3">
              <h2 class="text-lg sm:text-xl font-semibold">Daily Streams Δ (sum)</h2>
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
            <h2 class="text-lg sm:text-xl font-semibold mb-3">Playlists</h2>
            <label class="text-sm opacity-70">Select playlist</label>
            <select id="playlistSelect" class="w-full border rounded px-2 py-2 mt-1"></select>
            <div class="mt-4 text-sm">
              <div>Latest followers: <span id="plFollowers" class="font-semibold">-</span></div>
              <div>Last daily Δ: <span id="plDelta" class="font-semibold">-</span></div>
              <div class="mt-2"><a id="plLink" class="underline" target="_blank" rel="noopener">Open in Spotify</a></div>
            </div>
          </div>
        </section>

        <section class="grid grid-cols-1 lg:grid-cols-2 gap-6 mt-6">
          <div class="card">
            <div class="flex items-center justify-between mb-3">
              <h2 class="text-lg sm:text-xl font-semibold">Playlist Growth (followers & daily Δ)</h2>
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
              <h2 class="text-lg sm:text-xl font-semibold">Overlay: Streams Δ vs Total Followers Δ (+ cumulative)</h2>
              <div>
                <label class="mr-2 text-sm opacity-70">Window</label>
                <select id="overlayDays" class="border rounded px-2 py-1 text-sm">
                  <option value="30">30 days</option>
                  <option value="90" selected>90 days</option>
                  <option value="180">180 days</option>
                </select>
              </div>
            </div>
            <canvas id="overlayChart" height="120"></canvas>
          </div>
        </section>

        <section class="grid grid-cols-1 gap-6 mt-6">
          <div class="card">
            <div class="flex items-center justify-between mb-3">
              <h2 class="text-lg sm:text-xl font-semibold">Top track deltas by day</h2>
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
    </div>
  </div>

  <script>
    // Reveal app after a short beat (lets the text pop + ring wipe read)
    window.addEventListener('load', () => {
      setTimeout(() => document.body.classList.add('loaded'), 650);
    });

    const DEFAULT_PLAYLIST_NAME = {{ default_playlist_name | tojson }};
    let streamsChart, playlistChart, overlayChart;
    const fmt = (n) => Number(n).toLocaleString();

    async function api(path) {
      const r = await fetch(path);
      if (!r.ok) throw new Error(await r.text());
      return r.json();
    }

    // Streams (LINE chart, from streams.daily_delta)
    async function loadStreams(days) {
      const data = await api('/api/streams/total-daily?days=' + days);
      const ctx = document.getElementById('streamsChart').getContext('2d');
      const cfg = {
        type: 'line',
        data: { labels: data.labels, datasets: [{ label: 'Streams Δ (sum)', data: data.values, tension: 0.3, fill: false }] },
        options: {
          responsive: true,
          scales: { x: { ticks: { maxRotation: 0, autoSkip: true } }, y: { beginAtZero: true } },
          plugins: { tooltip: { callbacks: { label: (c) => ' ' + fmt(c.parsed.y) } } }
        }
      };
      if (streamsChart) streamsChart.destroy();
      streamsChart = new Chart(ctx, cfg);
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
      const a = document.getElementById('plLink'); a.href = p.web_url;
    }

    // Playlist chart
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
          responsive: true,
          scales: {
            x: { ticks: { maxRotation: 0, autoSkip: true } },
            y1: { type: 'linear', position: 'left', beginAtZero: true },
            y2: { type: 'linear', position: 'right', beginAtZero: true, grid: { drawOnChartArea: false } }
          },
          plugins: { tooltip: { callbacks: { label: (c) => ' ' + fmt(c.parsed.y) } } }
        }
      };
      if (playlistChart) playlistChart.destroy();
      playlistChart = new Chart(ctx, cfg);
      await updatePlaylistCard();
    }

    // Overlay: streams Δ vs followers Δ
    async function loadOverlay(days) {
      const data = await api('/api/overlay/streams-vs-followers?days=' + days);
      const ctx = document.getElementById('overlayChart').getContext('2d');
      const cfg = {
        data: { labels: data.labels, datasets: [
          { type: 'bar',  label: 'Streams Δ (sum)',          data: data.streams_delta,  yAxisID: 'yL' },
          { type: 'line', label: 'Followers Δ (total)',      data: data.followers_delta, yAxisID: 'yR', tension: 0.25 },
          { type: 'line', label: 'Followers Δ (cumulative)', data: data.followers_cum,   yAxisID: 'yR', tension: 0.25 }
        ]},
        options: {
          responsive: true,
          scales: {
            x: { ticks: { maxRotation: 0, autoSkip: true } },
            yL: { type: 'linear', position: 'left', beginAtZero: true },
            yR: { type: 'linear', position: 'right', beginAtZero: true, grid: { drawOnChartArea: false } }
          },
          plugins: { tooltip: { callbacks: { label: (c) => ' ' + fmt(c.parsed.y) } } }
        }
      };
      if (overlayChart) overlayChart.destroy();
      overlayChart = new Chart(ctx, cfg);
    }

    // Deltas-by-day table
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

    document.getElementById('streamsDays').addEventListener('change', e => loadStreams(e.target.value));
    document.getElementById('playlistDays').addEventListener('change', e => loadPlaylistChart(e.target.value));
    document.getElementById('overlayDays').addEventListener('change', e => loadOverlay(e.target.value));
    document.getElementById('playlistSelect').addEventListener('change', async () => {
      await updatePlaylistCard();
      await loadPlaylistChart(document.getElementById('playlistDays').value);
    });
    document.getElementById('btnReloadDeltas').addEventListener('click', loadDeltaTable);

    (async () => {
      await loadStreams(document.getElementById('streamsDays').value);
      await loadPlaylists();
      await loadOverlay(document.getElementById('overlayDays').value);
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


# ── API: streams (from streams.daily_delta) ────────────────────────────────────
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


# ── API: playlists (uses playlist_followers & playlist_followers_delta) ────────
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


# ── API: overlay (streams.daily_delta vs followers_delta) ──────────────────────
@app.get("/api/overlay/streams-vs-followers")
def api_overlay():
    days = _clamp_days(request.args.get("days"), 90)

    q_streams = """
        SELECT s.stream_date AS d,
               COALESCE(SUM(GREATEST(s.daily_delta, 0)), 0)::bigint AS v
        FROM streams s
        WHERE s.platform='spotify'
          AND s.stream_date >= CURRENT_DATE - %s::int * INTERVAL '1 day'
        GROUP BY s.stream_date
        ORDER BY s.stream_date
    """

    q_fdelta = """
        SELECT date AS d, COALESCE(SUM(delta),0)::bigint AS v
        FROM playlist_followers_delta
        WHERE date >= CURRENT_DATE - %s::int * INTERVAL '1 day'
        GROUP BY d
        ORDER BY d
    """

    rows_s = _q(q_streams, (days,))
    rows_f = _q(q_fdelta, (days,))

    dates = {r["d"] for r in rows_s} | {r["d"] for r in rows_f}
    if dates:
        start, end = min(dates), max(dates)
    else:
        end = date.today(); start = end - timedelta(days=days)

    labels, svals = _fill_series([(r["d"], int(r["v"])) for r in rows_s], start, end)
    _, fvals = _fill_series([(r["d"], int(r["v"])) for r in rows_f], start, end)

    fcum, total = [], 0
    for v in fvals:
        total += v
        fcum.append(total)

    return jsonify({
        "labels": labels,
        "streams_delta": svals,
        "followers_delta": fvals,
        "followers_cum": fcum,
    })


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
