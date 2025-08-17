# dashboard.py — standalone Flask dashboard for Streams & Playlist Growth

import os
from datetime import date, timedelta
from typing import List, Tuple
from flask import Flask, jsonify, render_template_string, request
from psycopg_pool import ConnectionPool, PoolClosed
from psycopg.rows import dict_row

LOCAL_TZ = os.getenv("LOCAL_TZ", "Europe/London")
DEFAULT_PLAYLIST_NAME = os.getenv("DEFAULT_PLAYLIST_NAME", "TOGI Motivation")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

# Outlier trimming for streams (per-track delta)
OUTLIER_PCTL = float(os.getenv("STREAMS_OUTLIER_PCTL", "0.995"))  # 99.5th percentile
DB_SSLMODE = os.getenv("DB_SSLMODE", "prefer")                    # set to "disable" on TCP proxy

POOL = ConnectionPool(conninfo=DATABASE_URL, kwargs={"sslmode": DB_SSLMODE},
                      min_size=1, max_size=5, open=False)

app = Flask(__name__)

# ── DB helpers ────────────────────────────────────────────────────────────────
def _ensure_pool_open():
    try: POOL.open()
    except Exception: pass

def _q(sql: str, params: tuple | None = None):
    _ensure_pool_open()
    try:
        with POOL.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()
    except PoolClosed:
        _ensure_pool_open()
        with POOL.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()

# ── Utils ─────────────────────────────────────────────────────────────────────
def _daterange(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]

def _fill_series(rows: list[tuple[date, int]], start: date, end: date):
    idx = {d: int(v) for d, v in rows}
    labels, values = [], []
    for d in _daterange(start, end):
        labels.append(d.isoformat())
        values.append(idx.get(d, 0))
    return labels, values

def _clamp_days(raw: str | None, default=30, lo=1, hi=365) -> int:
    try: v = int(raw or default)
    except: v = default
    return min(max(v, lo), hi)

# ── UI ────────────────────────────────────────────────────────────────────────
@app.get("/")
def ui():
    html = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Streams & Playlists Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script src="https://cdn.tailwindcss.com"></script>
<style>
  .card { background: rgba(255,255,255,0.7); border-radius: 1rem; box-shadow: 0 8px 24px rgba(0,0,0,0.08); padding: 1.25rem; }
  @media (prefers-color-scheme: dark) { .card { background: rgba(24,24,27,0.7); color:#fff; } }
  body { background: radial-gradient(1200px 600px at 10% -10%, #f0f9ff 0%, transparent 60%),
                  radial-gradient(1200px 600px at 110% -10%, #fef3c7 0%, transparent 60%),
                  radial-gradient(1200px 600px at 50% 120%, #e9d5ff 0%, transparent 60%);
         min-height: 100vh; }
</style>
</head>
<body class="text-zinc-900 dark:text-zinc-100">
<div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
  <header class="mb-8">
    <h1 class="text-2xl sm:text-3xl font-bold">Streams & Playlists Dashboard</h1>
    <p class="text-sm opacity-70">Timezone: {{ local_tz }}</p>
  </header>

  <section class="grid grid-cols-1 lg:grid-cols-3 gap-6">
    <div class="card col-span-1 lg:col-span-2">
      <div class="flex items-center justify-between mb-3">
        <h2 class="text-xl font-semibold">Daily Streams Δ (sum)</h2>
        <div>
          <label class="mr-2 text-sm opacity-70">Window</label>
          <select id="streamsDays" class="border rounded px-2 py-1 text-sm">
            <option value="30" selected>30 days</option>
            <option value="90">90 days</option>
            <option value="180">180 days</option>
          </select>
        </div>
      </div>
      <canvas id="streamsChart" height="110"></canvas>
    </div>

    <div class="card">
      <h2 class="text-xl font-semibold mb-3">Playlists</h2>
      <label class="text-sm opacity-70">Select playlist</label>
      <select id="playlistSelect" class="w-full border rounded px-2 py-2 mt-1"></select>
      <div class="mt-4 text-sm">
        <div>Latest followers: <span id="plFollowers" class="font-semibold">-</span></div>
        <div>Last daily Δ: <span id="plDelta" class="font-semibold">-</span></div>
        <div class="mt-2"><a id="plLink" class="underline text-blue-600" target="_blank" rel="noopener">Open in Spotify</a></div>
      </div>
    </div>
  </section>

  <section class="grid grid-cols-1 lg:grid-cols-2 gap-6 mt-6">
    <div class="card">
      <div class="flex items-center justify-between mb-3">
        <h2 class="text-xl font-semibold">Playlist Growth (followers & daily Δ)</h2>
        <div>
          <label class="mr-2 text-sm opacity-70">Window</label>
          <select id="playlistDays" class="border rounded px-2 py-1 text-sm">
            <option value="30" selected>30 days</option>
            <option value="90">90 days</option>
            <option value="180">180 days</option>
          </select>
        </div>
      </div>
      <canvas id="playlistChart" height="120"></canvas>
    </div>

    <div class="card">
      <div class="flex items-center justify-between mb-3">
        <h2 class="text-xl font-semibold">Overlay: Streams Δ vs Total Followers Δ (+ cumulative)</h2>
        <div>
          <label class="mr-2 text-sm opacity-70">Window</label>
          <select id="overlayDays" class="border rounded px-2 py-1 text-sm">
            <option value="30" selected>30 days</option>
            <option value="90">90 days</option>
            <option value="180">180 days</option>
          </select>
        </div>
      </div>
      <canvas id="overlayChart" height="120"></canvas>
    </div>
  </section>
</div>

<script>
  const DEFAULT_PLAYLIST_NAME = {{ default_playlist_name | tojson }};
  let streamsChart, playlistChart, overlayChart;
  const fmt = (n) => Number(n).toLocaleString();
  async function api(path){ const r=await fetch(path); if(!r.ok) throw new Error(await r.text()); return r.json(); }

  async function loadStreams(days){
    const data = await api('/api/streams/total-daily?days='+days);
    const ctx = document.getElementById('streamsChart').getContext('2d');
    const cfg = { type:'line', data:{ labels:data.labels, datasets:[{ type:'line', label:'Streams Δ (sum)', data:data.values }]},
      options:{ responsive:true, scales:{ x:{ ticks:{ maxRotation:0, autoSkip:true }}, y:{ beginAtZero:true }},
        plugins:{ tooltip:{ callbacks:{ label:(c)=>' '+fmt(c.parsed.y) }}}}};
    if(streamsChart) streamsChart.destroy(); streamsChart = new Chart(ctx, cfg);
  }

  async function loadPlaylists(){
    const list = await api('/api/playlists/list');
    const sel = document.getElementById('playlistSelect'); sel.innerHTML='';
    let defaultId = list.length ? list[0].playlist_id : null;
    for(const p of list){ const o=document.createElement('option'); o.value=p.playlist_id; o.textContent=p.playlist_name||p.playlist_id; sel.appendChild(o); }
    const def = list.find(p => (p.playlist_name||'').toLowerCase().startsWith(DEFAULT_PLAYLIST_NAME.toLowerCase()));
    if(def) defaultId = def.playlist_id; if(defaultId) sel.value = defaultId;
    await updatePlaylistCard(); await loadPlaylistChart(document.getElementById('playlistDays').value);
  }

  async function updatePlaylistCard(){
    const list = await api('/api/playlists/list');
    const id = document.getElementById('playlistSelect').value;
    const p = list.find(x => x.playlist_id===id); if(!p) return;
    document.getElementById('plFollowers').textContent = fmt(p.followers ?? 0);
    document.getElementById('plDelta').textContent = (p.delta==null)?'-':fmt(p.delta);
    document.getElementById('plLink').href = p.web_url;
  }

  async function loadPlaylistChart(days){
    const id = document.getElementById('playlistSelect').value; if(!id) return;
    const data = await api('/api/playlists/'+encodeURIComponent(id)+'/series?days='+days);
    const ctx = document.getElementById('playlistChart').getContext('2d');
    const cfg = { data:{ labels:data.labels, datasets:[
        { type:'line', label:'Followers', data:data.followers, yAxisID:'y1' },
        { type:'bar',  label:'Daily Δ',  data:data.deltas,    yAxisID:'y2' }
      ]},
      options:{ responsive:true, scales:{
        x:{ ticks:{ maxRotation:0, autoSkip:true }},
        y1:{ type:'linear', position:'left', beginAtZero:true },
        y2:{ type:'linear', position:'right', beginAtZero:true, grid:{ drawOnChartArea:false }}
      }, plugins:{ tooltip:{ callbacks:{ label:(c)=>' '+fmt(c.parsed.y) }}}}};
    if(playlistChart) playlistChart.destroy(); playlistChart = new Chart(ctx, cfg);
    await updatePlaylistCard();
  }

  async function loadOverlay(days){
    const data = await api('/api/overlay/streams-vs-followers?days='+days);
    const ctx = document.getElementById('overlayChart').getContext('2d');
    const cfg = { data:{ labels:data.labels, datasets:[
        { type:'line', label:'Streams Δ (sum)', data:data.streams_delta, yAxisID:'yL' },
        { type:'line', label:'Followers Δ (total)', data:data.followers_delta, yAxisID:'yR' },
        { type:'line', label:'Followers Δ (cumulative)', data:data.followers_cum, yAxisID:'yR' }
      ]},
      options:{ responsive:true, scales:{
        x:{ ticks:{ maxRotation:0, autoSkip:true }},
        yL:{ type:'linear', position:'left', beginAtZero:true },
        yR:{ type:'linear', position:'right', beginAtZero:true, grid:{ drawOnChartArea:false }}
      }, plugins:{ tooltip:{ callbacks:{ label:(c)=>' '+fmt(c.parsed.y) }}}}};
    if(overlayChart) overlayChart.destroy(); overlayChart = new Chart(ctx, cfg);
  }

  document.getElementById('streamsDays').addEventListener('change', e=>loadStreams(e.target.value));
  document.getElementById('playlistDays').addEventListener('change', e=>loadPlaylistChart(e.target.value));
  document.getElementById('overlayDays').addEventListener('change', e=>loadOverlay(e.target.value));
  document.getElementById('playlistSelect').addEventListener('change', async()=>{ await updatePlaylistCard(); await loadPlaylistChart(document.getElementById('playlistDays').value); });

  (async()=>{ await loadStreams(document.getElementById('streamsDays').value); await loadPlaylists(); await loadOverlay(document.getElementById('overlayDays').value); })();
</script>
</body></html>
"""
    return render_template_string(html, local_tz=LOCAL_TZ, default_playlist_name=DEFAULT_PLAYLIST_NAME)

# ── API: Streams with outlier trimming ────────────────────────────────────────
@app.get("/api/streams/total-daily")
def api_streams_total_daily():
    days = _clamp_days(request.args.get("days"), 30)
    q = f"""
        WITH s AS (
          SELECT t.isrc,
                 s.stream_date AS d,
                 s.playcount,
                 LAG(s.playcount)  OVER (PARTITION BY t.isrc ORDER BY s.stream_date) AS prev_pc,
                 LAG(s.stream_date) OVER (PARTITION BY t.isrc ORDER BY s.stream_date) AS prev_d
          FROM public.streams s
          JOIN public.track_dim t USING (track_uid)
          WHERE s.platform = 'spotify'
            AND s.stream_date >= CURRENT_DATE - %s::int * INTERVAL '1 day'
        ),
        deltas AS (
          SELECT d,
                 GREATEST(CASE WHEN prev_d = d - INTERVAL '1 day' THEN (playcount - prev_pc) ELSE 0 END, 0)::bigint AS delta
          FROM s
        ),
        pctl AS (
          SELECT PERCENTILE_CONT({OUTLIER_PCTL}) WITHIN GROUP (ORDER BY delta) AS p
          FROM deltas
        )
        SELECT d, COALESCE(SUM(CASE WHEN delta <= pctl.p THEN delta ELSE 0 END),0)::bigint AS v
        FROM deltas, pctl
        GROUP BY d
        ORDER BY d
    """
    rows = _q(q, (days,))
    if rows:
        start, end = rows[0]["d"], rows[-1]["d"]
    else:
        end = date.today(); start = end - timedelta(days=days)
    labels, values = _fill_series([(r["d"], r["v"]) for r in rows], start, end)
    return jsonify({"labels": labels, "values": values})

# ── Spike inspector (who made the jump?) ──────────────────────────────────────
@app.get("/api/streams/top-spikes")
def api_streams_top_spikes():
    day = request.args.get("date")  # YYYY-MM-DD
    limit = int(request.args.get("limit", 20))
    if not day:  # default to latest date we have
        rows = _q("SELECT MAX(stream_date)::date AS d FROM public.streams WHERE platform='spotify'")
        day = rows[0]["d"].isoformat() if rows and rows[0]["d"] else date.today().isoformat()

    q = """
        WITH s AS (
          SELECT t.isrc, t.title, t.artist,
                 s.stream_date AS d, s.playcount,
                 LAG(s.playcount)  OVER (PARTITION BY t.isrc ORDER BY s.stream_date) AS prev_pc,
                 LAG(s.stream_date) OVER (PARTITION BY t.isrc ORDER BY s.stream_date) AS prev_d
          FROM public.streams s
          JOIN public.track_dim t USING (track_uid)
          WHERE s.platform='spotify'
        )
        SELECT isrc, title, artist,
               GREATEST(CASE WHEN prev_d = d - INTERVAL '1 day' THEN (playcount - prev_pc) ELSE 0 END, 0)::bigint AS delta
        FROM s
        WHERE d = %s::date
        ORDER BY delta DESC
        LIMIT %s
    """
    rows = _q(q, (day, limit))
    return jsonify({"date": day, "rows": rows})

# ── Playlists & overlay (unchanged) ───────────────────────────────────────────
@app.get("/api/playlists/list")
def api_playlists_list():
    q = """
        WITH latest AS (
          SELECT DISTINCT ON (playlist_id)
            playlist_id, playlist_name, followers, snapshot_date
          FROM public.playlist_followers
          WHERE platform = 'spotify'
          ORDER BY playlist_id, snapshot_date DESC
        )
        SELECT
          l.playlist_id, l.playlist_name, l.followers, d.delta, l.snapshot_date AS date,
          'https://open.spotify.com/playlist/' || split_part(l.playlist_id, ':', 3) AS web_url
        FROM latest l
        LEFT JOIN public.playlist_followers_delta d
          ON d.playlist_id = l.playlist_id AND d.date = l.snapshot_date
        ORDER BY l.followers DESC;
    """
    rows = _q(q)
    return jsonify([{
        "playlist_id": r["playlist_id"],
        "playlist_name": r["playlist_name"],
        "followers": int(r["followers"]) if r["followers"] is not None else None,
        "delta": int(r["delta"]) if r["delta"] is not None else None,
        "date": r["date"].isoformat() if r["date"] else None,
        "web_url": r["web_url"],
    } for r in rows])

@app.get("/api/playlists/<path:playlist_id>/series")
def api_playlist_series(playlist_id: str):
    days = _clamp_days(request.args.get("days"), 30)
    q = """
        SELECT date AS d, followers, delta
        FROM public.playlist_followers_delta
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

@app.get("/api/overlay/streams-vs-followers")
def api_overlay():
    days = _clamp_days(request.args.get("days"), 30)
    q_streams = f"""
        WITH s AS (
          SELECT t.isrc, s.stream_date AS d, s.playcount,
                 LAG(s.playcount) OVER (PARTITION BY t.isrc ORDER BY s.stream_date) AS prev_pc,
                 LAG(s.stream_date) OVER (PARTITION BY t.isrc ORDER BY s.stream_date) AS prev_d
          FROM public.streams s
          JOIN public.track_dim t USING (track_uid)
          WHERE s.platform='spotify'
            AND s.stream_date >= CURRENT_DATE - %s::int * INTERVAL '1 day'
        ),
        deltas AS (
          SELECT d, GREATEST(CASE WHEN prev_d = d - INTERVAL '1 day' THEN (playcount - prev_pc) ELSE 0 END, 0)::bigint AS delta
          FROM s
        ),
        pctl AS (SELECT PERCENTILE_CONT({OUTLIER_PCTL}) WITHIN GROUP (ORDER BY delta) AS p FROM deltas),
        daily AS (
          SELECT d, COALESCE(SUM(CASE WHEN delta <= pctl.p THEN delta ELSE 0 END),0)::bigint AS v
          FROM deltas, pctl
          GROUP BY d
        )
        SELECT d, v FROM daily ORDER BY d
    """
    q_fdelta = """
        SELECT date AS d, COALESCE(SUM(delta),0)::bigint AS v
        FROM public.playlist_followers_delta
        WHERE date >= CURRENT_DATE - %s::int * INTERVAL '1 day'
        GROUP BY d ORDER BY d
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
    return jsonify({"labels": labels, "streams_delta": svals, "followers_delta": fvals, "followers_cum": fcum})

# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    try:
        ssl_row = _q("SHOW ssl")
        ssl = ssl_row[0]["ssl"] if ssl_row else "unknown"
        _q("SELECT NOW()")
        return {"ok": True, "db": True, "ssl": ssl}
    except Exception as e:
        return {"ok": False, "db": False, "error": str(e)}

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, threaded=True)
