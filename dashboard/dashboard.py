# dashboard.py — standalone Flask dashboard (gap-adjusted deltas via views)

import os
from datetime import date, timedelta
from flask import Flask, jsonify, render_template_string, request
from psycopg_pool import ConnectionPool, PoolClosed
from psycopg.rows import dict_row

# ── Config ────────────────────────────────────────────────────────────────────
LOCAL_TZ = os.getenv("LOCAL_TZ", "Europe/London")
DEFAULT_PLAYLIST_NAME = os.getenv("DEFAULT_PLAYLIST_NAME", "TOGI Motivation")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

DB_SSLMODE = os.getenv("DB_SSLMODE", "prefer")  # set "disable" if your TCP proxy needs it

POOL = ConnectionPool(conninfo=DATABASE_URL, kwargs={"sslmode": DB_SSLMODE},
                      min_size=1, max_size=5, open=False)

app = Flask(__name__)

# ── DB helpers ────────────────────────────────────────────────────────────────
def _ensure_pool_open():
    try:
        POOL.open()
    except Exception:
        pass

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

def _exec(sql: str):
    _ensure_pool_open()
    with POOL.connection() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()

def _clamp_days(raw: str | None, default=30, lo=1, hi=365) -> int:
    try:
        v = int(raw or default)
    except:
        v = default
    return min(max(v, lo), hi)

def _fill_series(rows, start: date, end: date):
    idx = {d: int(v) for d, v in rows}
    labels, values = [], []
    days = (end - start).days
    for i in range(days + 1):
        d = start + timedelta(days=i)
        labels.append(d.isoformat())
        values.append(idx.get(d, 0))
    return labels, values

# ── One-time view creation (idempotent) ───────────────────────────────────────
CREATE_VIEWS_SQL = r"""
-- Per-track, per-day gap-adjusted deltas:
-- Keep dates as DATE to get integer day gaps, then distribute across gap days.

CREATE OR REPLACE VIEW public.spotify_streams_delta_adjusted AS
WITH base AS (
  SELECT
    s.track_uid,
    s.stream_date::date AS d,                         -- DATE
    s.playcount::bigint AS playcount,
    LAG(s.playcount)   OVER (PARTITION BY s.track_uid ORDER BY s.stream_date) AS prev_pc,
    LAG(s.stream_date) OVER (PARTITION BY s.track_uid ORDER BY s.stream_date) AS prev_d   -- DATE
  FROM public.streams s
  WHERE s.platform = 'spotify'
),
norm AS (
  SELECT
    track_uid,
    d,
    playcount,
    COALESCE(prev_pc, playcount)             AS prev_pc_norm,
    COALESCE(prev_d, d - 1)::date            AS prev_d_norm   -- ensure DATE; first snapshot uses d-1
  FROM base
),
spans AS (
  SELECT
    track_uid,
    d,
    prev_d_norm AS prev_d,                   -- DATE
    GREATEST(playcount - prev_pc_norm, 0)::numeric AS inc,
    GREATEST((d - prev_d_norm), 1)           AS days_in_span  -- INTEGER days (DATE - DATE)
  FROM norm
),
distributed AS (
  SELECT
    s.track_uid,
    (gs)::date AS date,
    CASE WHEN s.days_in_span > 0 THEN s.inc / s.days_in_span ELSE 0 END AS delta_share
  FROM spans s
  JOIN LATERAL generate_series(
        (s.prev_d::timestamp) + interval '1 day',
        (s.d::timestamp),
        interval '1 day'
      ) gs ON true
)
SELECT track_uid, date, delta_share
FROM distributed;

-- Daily sum of gap-adjusted deltas:
CREATE OR REPLACE VIEW public.spotify_streams_daily_adjusted AS
SELECT date, SUM(delta_share)::bigint AS delta
FROM public.spotify_streams_delta_adjusted
GROUP BY date;
"""

def _ensure_views():
    _exec(CREATE_VIEWS_SQL)

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
  table { width:100%; border-collapse: collapse; }
  th, td { padding: 0.5rem; border-bottom: 1px solid rgba(0,0,0,0.06); }
  .muted { opacity:.7 }
</style>
</head>
<body class="text-zinc-900 dark:text-zinc-100">
<div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
  <header class="mb-8">
    <h1 class="text-2xl sm:text-3xl font-bold">Streams & Playlists Dashboard</h1>
    <p class="text-sm muted">Timezone: {{ local_tz }}</p>
  </header>

  <section class="grid grid-cols-1 lg:grid-cols-3 gap-6">
    <div class="card col-span-1 lg:col-span-2">
      <div class="flex items-center justify-between mb-3">
        <h2 class="text-xl font-semibold">Daily Streams Δ (sum)</h2>
        <div>
          <label class="mr-2 text-sm muted">Window</label>
          <select id="streamsDays" class="border rounded px-2 py-1 text-sm">
            <option value="30" selected>30 days</option>
            <option value="90">90 days</option>
            <option value="180">180 days</option>
          </select>
        </div>
      </div>
      <canvas id="streamsChart" height="110"></canvas>
      <p class="text-xs muted mt-2">Click a point to see that day's top track deltas below.</p>
    </div>

    <div class="card">
      <h2 class="text-xl font-semibold mb-3">Playlists</h2>
      <label class="text-sm muted">Select playlist</label>
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
          <label class="mr-2 text-sm muted">Window</label>
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
          <label class="mr-2 text-sm muted">Window</label>
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

  <section class="grid grid-cols-1 gap-6 mt-6">
    <div class="card">
      <div class="flex items-center justify-between mb-3">
        <h2 class="text-xl font-semibold">Top track deltas (by day)</h2>
        <div class="flex items-center gap-3">
          <label class="text-sm muted">Date</label>
          <select id="leadersDate" class="border rounded px-2 py-1 text-sm"></select>
        </div>
      </div>
      <div class="overflow-x-auto">
        <table id="leadersTable">
          <thead>
            <tr class="text-left text-sm muted">
              <th>#</th><th>ISRC</th><th>Title</th><th>Artist</th><th>Δ</th><th>Prev → Today</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
      <p class="text-xs muted mt-2">Δ is gap-adjusted (evenly distributed across missed days) with no outlier filtering.</p>
    </div>
  </section>
</div>

<script>
  const DEFAULT_PLAYLIST_NAME = {{ default_playlist_name | tojson }};
  let streamsChart, playlistChart, overlayChart, streamsLabels = [];
  const fmt = (n) => Number(n).toLocaleString();
  async function api(path){ const r=await fetch(path); if(!r.ok) throw new Error(await r.text()); return r.json(); }

  // Streams
  async function loadStreams(days){
    const data = await api('/api/streams/total-daily?days='+days);
    streamsLabels = data.labels;
    const ctx = document.getElementById('streamsChart').getContext('2d');
    const cfg = { type:'line', data:{ labels:data.labels, datasets:[{ label:'Streams Δ (sum)', data:data.values }]},
      options:{ responsive:true, onClick:(_,els)=>{ if(els.length){ const i=els[0].index; const d=streamsLabels[i]; setLeadersDate(d); loadLeaders(); } },
        scales:{ x:{ ticks:{ maxRotation:0, autoSkip:true }}, y:{ beginAtZero:true }},
        plugins:{ tooltip:{ callbacks:{ label:(c)=>' '+fmt(c.parsed.y) }}}};
    if(streamsChart) streamsChart.destroy(); streamsChart = new Chart(ctx, cfg);
  }

  // Playlists
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
    if(playlistChart) playlistChart.destroy(); playlistChart = new Chart(ctx, cfg); await updatePlaylistCard();
  }

  // Overlay
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

  // Leaders panel
  async function loadAvailableDates(){
    const r = await api('/api/streams/available-dates?days=90');
    const sel = document.getElementById('leadersDate'); sel.innerHTML='';
    for(const d of r.dates){ const o=document.createElement('option'); o.value=d; o.textContent=d; sel.appendChild(o); }
    if(r.dates.length){ sel.value = r.dates[r.dates.length-1]; }
  }
  function setLeadersDate(d){ const sel=document.getElementById('leadersDate'); if([...sel.options].some(o=>o.value===d)) sel.value=d; }
  async function loadLeaders(){
    const d = document.getElementById('leadersDate').value;
    const r = await api('/api/streams/daily-leaders?date='+d+'&limit=25');
    const tb = document.querySelector('#leadersTable tbody'); tb.innerHTML='';
    r.rows.forEach((row, i) => {
      const tr = document.createElement('tr');
      tr.innerHTML = `<td class="muted">${i+1}</td>
        <td>${row.isrc||''}</td>
        <td>${(row.title||'').replaceAll('<','&lt;')}</td>
        <td>${(row.artist||'').replaceAll('<','&lt;')}</td>
        <td><strong>${fmt(row.delta)}</strong></td>
        <td class="muted">${fmt(row.prev_pc)} → ${fmt(row.playcount)}</td>`;
      tb.appendChild(tr);
    });
  }

  // Events
  document.getElementById('streamsDays').addEventListener('change', e=>loadStreams(e.target.value));
  document.getElementById('playlistDays').addEventListener('change', e=>loadPlaylistChart(e.target.value));
  document.getElementById('overlayDays').addEventListener('change', e=>loadOverlay(e.target.value));
  document.getElementById('playlistSelect').addEventListener('change', async()=>{ await updatePlaylistCard(); await loadPlaylistChart(document.getElementById('playlistDays').value); });
  document.getElementById('leadersDate').addEventListener('change', loadLeaders);

  // Boot
  (async()=>{
    await loadStreams(document.getElementById('streamsDays').value);
    await loadPlaylists();
    await loadOverlay(document.getElementById('overlayDays').value);
    await loadAvailableDates();
    await loadLeaders();
  })();
</script>
</body></html>
"""
    return render_template_string(html, local_tz=LOCAL_TZ, default_playlist_name=DEFAULT_PLAYLIST_NAME)

# ── API (uses the views) ──────────────────────────────────────────────────────
@app.before_request
def _views_guard():
    _ensure_views()

@app.get("/api/streams/total-daily")
def api_streams_total_daily():
    days = _clamp_days(request.args.get("days"), 30)
    q = """
      SELECT date AS d, delta AS v
      FROM public.spotify_streams_daily_adjusted
      WHERE date >= CURRENT_DATE - %s::int * INTERVAL '1 day'
      ORDER BY d;
    """
    rows = _q(q, (days,))
    if rows:
        start, end = rows[0]["d"], rows[-1]["d"]
    else:
        end = date.today(); start = end - timedelta(days=days)
    labels, values = _fill_series([(r["d"], r["v"]) for r in rows], start, end)
    return jsonify({"labels": labels, "values": values})

@app.get("/api/streams/available-dates")
def api_streams_available_dates():
    days = _clamp_days(request.args.get("days"), 90)
    q = """
      SELECT date AS day
      FROM public.spotify_streams_daily_adjusted
      WHERE date >= CURRENT_DATE - %s::int * INTERVAL '1 day'
      ORDER BY day;
    """
    rows = _q(q, (days,))
    return jsonify({"dates": [r["day"].isoformat() for r in rows]})

@app.get("/api/streams/daily-leaders")
def api_streams_daily_leaders():
    day = request.args.get("date") or date.today().isoformat()
    limit = int(request.args.get("limit", 25))
    q = """
      WITH d AS (
        SELECT sda.track_uid, sda.delta_share::bigint AS delta
        FROM public.spotify_streams_delta_adjusted sda
        WHERE sda.date = %s::date
      ),
      prevsnap AS (
        SELECT x.track_uid,
               (SELECT s2.playcount FROM public.streams s2
                WHERE s2.platform='spotify' AND s2.track_uid=x.track_uid AND s2.stream_date <= %s::date
                ORDER BY s2.stream_date DESC LIMIT 1) AS prev_pc
        FROM d x
      ),
      todaysnap AS (
        SELECT x.track_uid,
               (SELECT s2.playcount FROM public.streams s2
                WHERE s2.platform='spotify' AND s2.track_uid=x.track_uid AND s2.stream_date >= %s::date
                ORDER BY s2.stream_date ASC LIMIT 1) AS playcount
        FROM d x
      )
      SELECT t.isrc, t.title, t.artist,
             COALESCE(ts.playcount, ps.prev_pc, 0)::bigint AS playcount,
             COALESCE(ps.prev_pc, 0)::bigint            AS prev_pc,
             d.delta
      FROM d
      JOIN public.track_dim t USING (track_uid)
      LEFT JOIN prevsnap ps USING (track_uid)
      LEFT JOIN todaysnap ts USING (track_uid)
      ORDER BY d.delta DESC
      LIMIT %s;
    """
    rows = _q(q, (day, day, day, limit))
    return jsonify({"date": day, "rows": rows})

@app.get("/api/playlists/list")
def api_playlists_list():
    q = """
        WITH latest AS (
          SELECT DISTINCT ON (playlist_id)
            playlist_id, playlist_name, followers, snapshot_date
          FROM public.playlist_followers
          WHERE platform='spotify'
          ORDER BY playlist_id, snapshot_date DESC
        )
        SELECT l.playlist_id, l.playlist_name, l.followers, d.delta, l.snapshot_date AS date,
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
    q_streams = """
      SELECT date AS d, delta AS v
      FROM public.spotify_streams_daily_adjusted
      WHERE date >= CURRENT_DATE - %s::int * INTERVAL '1 day'
      ORDER BY d;
    """
    q_fdelta = """
      SELECT date AS d, COALESCE(SUM(delta),0)::bigint AS v
      FROM public.playlist_followers_delta
      WHERE date >= CURRENT_DATE - %s::int * INTERVAL '1 day'
      GROUP BY d
      ORDER BY d;
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
