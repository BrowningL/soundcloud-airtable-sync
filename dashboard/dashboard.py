# dashboard.py — KAIZEN-styled Flask dashboard (Streams, Playlists & Catalogue Size)
import os
from datetime import date, datetime, timedelta
from typing import List, Tuple, Dict, Any
from collections import defaultdict
import json
import urllib.parse
import urllib.request

from flask import Flask, jsonify, render_template_string, request
from psycopg_pool import ConnectionPool, PoolClosed
from psycopg.rows import dict_row

# ── Config ────────────────────────────────────────────────────────────────────
LOCAL_TZ = os.getenv("LOCAL_TZ", "Europe/London")
DEFAULT_PLAYLIST_NAME = os.getenv("DEFAULT_PLAYLIST_NAME", "TOGI Motivation")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Catalogue")
AIRTABLE_RELEASE_DATE_FIELD = os.getenv("AIRTABLE_RELEASE_DATE_FIELD", "Release Date")
AIRTABLE_DISTRIBUTOR_FIELD = os.getenv("AIRTABLE_DISTRIBUTOR_FIELD", "Distributor")
AIRTABLE_EXCLUDED_DISTRIBUTOR = os.getenv("AIRTABLE_EXCLUDED_DISTRIBUTOR", "External")
AIRTABLE_CACHE_TTL_SECS = int(os.getenv("AIRTABLE_CACHE_TTL_SECS", "21600"))  # 6 hours

POOL = ConnectionPool(
    conninfo=DATABASE_URL,
    kwargs={"sslmode": os.getenv("DB_SSLMODE", "prefer")},  # "disable" if needed
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

def _clamp_days(raw: str | None, default: int = 90, min_d: int = 1, max_d: int = 3650) -> int:
    try:
        v = int(raw or default)
    except Exception:
        v = default
    return min(max(v, min_d), max_d)

# ── Airtable helpers (Release Date -> catalogue size) ─────────────────────────
_airtable_cache: Dict[str, Any] = {
    "at": None,           # datetime when cached
    "labels": None,       # list[str]
    "values": None,       # list[int]
    "min_date": None,     # date
    "max_date": None,     # date
    "count": 0,           # total records counted
    "ok": False,
    "error": None,
}

def _airtable_enabled() -> bool:
    return bool(AIRTABLE_API_KEY and AIRTABLE_BASE_ID)

def _http_get_json(url: str, headers: Dict[str, str], params: Dict[str, Any], timeout: float = 30.0) -> Dict[str, Any]:
    query = urllib.parse.urlencode(
        [(k, v) for k, vv in params.items() for v in (vv if isinstance(vv, list) else [vv])]
    )
    full = f"{url}?{query}" if query else url
    req = urllib.request.Request(full, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

def _fetch_airtable_release_dates() -> Dict[str, Any]:
    """
    Fetch all release dates from Airtable, excluding records with Distributor == 'External',
    then build a cumulative series per day.
    """
    now = datetime.utcnow()
    if _airtable_cache["at"] and (now - _airtable_cache["at"]).total_seconds() < AIRTABLE_CACHE_TTL_SECS:
        return {
            "labels": _airtable_cache["labels"],
            "values": _airtable_cache["values"],
            "min_date": _airtable_cache["min_date"],
            "max_date": _airtable_cache["max_date"],
            "count": _airtable_cache["count"],
            "ok": _airtable_cache["ok"],
            "error": _airtable_cache["error"],
        }

    if not _airtable_enabled():
        out = {"labels": [], "values": [], "min_date": None, "max_date": None, "count": 0, "ok": False, "error": "Airtable not configured"}
        _airtable_cache.update(out); _airtable_cache["at"] = now
        return out

    base_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{urllib.parse.quote(AIRTABLE_TABLE_NAME)}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}

    # filter: must have a Release Date AND Distributor != 'External'
    # You can tweak logic if blank distributor should also be included (current formula includes blanks).
    formula = f"AND({{{{{ {AIRTABLE_RELEASE_DATE_FIELD} }}}}}, NOT({{{{{ {AIRTABLE_DISTRIBUTOR_FIELD} }}}}} = '{AIRTABLE_EXCLUDED_DISTRIBUTOR}'))"

    params = {
        "pageSize": 100,
        "fields[]": AIRTABLE_RELEASE_DATE_FIELD,  # small payload
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
                fields = rec.get("fields", {})
                raw = fields.get(AIRTABLE_RELEASE_DATE_FIELD)
                if not raw:
                    continue
                # Accept date-only ("YYYY-MM-DD") or ISO datetime
                try:
                    d = date.fromisoformat(raw[:10])
                except Exception:
                    try:
                        d = datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
                    except Exception:
                        continue
                per_day_additions[d] += 1
                total += 1
                if (seen_min is None) or (d < seen_min):
                    seen_min = d
                if (seen_max is None) or (d > seen_max):
                    seen_max = d

            offset = data.get("offset")
            if not offset:
                break

        if total == 0 or seen_min is None or seen_max is None:
            out = {"labels": [], "values": [], "min_date": None, "max_date": None, "count": 0, "ok": True, "error": None}
            _airtable_cache.update(out); _airtable_cache["at"] = now
            return out

        start = seen_min
        end = max(seen_max, date.today())
        labels, values = [], []
        running = 0
        for d in _daterange(start, end):
            running += per_day_additions.get(d, 0)
            labels.append(d.isoformat())
            values.append(running)

        out = {"labels": labels, "values": values, "min_date": start, "max_date": end, "count": total, "ok": True, "error": None}
        _airtable_cache.update(out); _airtable_cache["at"] = now
        return out

    except Exception as e:
        out = {"labels": [], "values": [], "min_date": None, "max_date": None, "count": 0, "ok": False, "error": str(e)}
        _airtable_cache.update(out); _airtable_cache["at"] = now
        return out

# ── UI ────────────────────────────────────────────────────────────────────────
@app.get("/")
def ui():
    html = f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>KAIZEN Dashboard — Streams, Playlists & Catalogue</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    :root {{
      --bg: #0b0b0f; --card: #12121a; --ink: #f2f2f7; --muted: #9aa0a6; --grid: #1d1d27;
    }}
    html, body {{ margin:0; padding:0; background:var(--bg); color:var(--ink);
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, Apple Color Emoji, Segoe UI Emoji; }}
    .wrap {{ max-width: 1280px; margin: 28px auto; padding: 0 16px; }}
    h1 {{ margin: 0 0 8px; font-weight: 800; }}
    .sub {{ color: var(--muted); margin-bottom: 20px; }}
    .grid {{ display: grid; gap: 16px; grid-template-columns: 1fr; }}
    @media (min-width: 1000px) {{ .grid.two {{ grid-template-columns: 1fr 1fr; }} }}
    .card {{ background: var(--card); border-radius: 18px; padding: 16px 16px 8px; box-shadow: 0 10px 30px rgba(0,0,0,.25); }}
    .card h3 {{ margin: 0 0 8px; font-weight: 700; }}
    canvas {{ width: 100%; height: 340px; }}
    .meta {{ color: var(--muted); font-size: 12px; margin-top: 8px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>KAIZEN Streams & Growth</h1>
    <div class="sub">TZ: {LOCAL_TZ} &middot; Default playlist: {DEFAULT_PLAYLIST_NAME}</div>

    <div class="grid two">
      <div class="card">
        <h3>All Playlists — Followers</h3>
        <canvas id="playlistsFollowers"></canvas>
        <div class="meta">Shows follower counts across all tracked playlists.</div>
      </div>
      <div class="card">
        <h3>All Playlists — Daily Follower Deltas</h3>
        <canvas id="playlistsDeltas"></canvas>
        <div class="meta">Day-over-day follower change across playlists.</div>
      </div>
    </div>

    <div class="grid" style="margin-top: 16px;">
      <div class="card">
        <h3>Catalogue Size Over Time</h3>
        <canvas id="catalogueChart"></canvas>
        <div id="catalogueMeta" class="meta"></div>
      </div>
    </div>
  </div>

  <script>
    async function jget(url) {{
      const r = await fetch(url);
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    }}

    // Followers (multi-series)
    (async () => {{
      try {{
        const data = await jget("/api/playlists/all-series?days=365");
        const ctx = document.getElementById("playlistsFollowers");
        if (ctx) {{
          const ds = (data.series || []).map((s) => ({{
            label: s.name, data: s.values, borderWidth: 2, fill: false, tension: 0.25
          }}));
          new Chart(ctx, {{
            type: "line",
            data: {{ labels: data.labels, datasets: ds }},
            options: {{
              responsive: true, interaction: {{ mode:"index", intersect:false }},
              plugins: {{ legend: {{ display: true }}, tooltip: {{ enabled: true }} }},
              scales: {{
                x: {{ grid: {{ color: "rgba(255,255,255,0.05)" }} }},
                y: {{ grid: {{ color: "rgba(255,255,255,0.05)" }}, beginAtZero: true }}
              }}
            }}
          }});
        }}
      }} catch(e) {{ console.error("Followers chart error:", e); }}
    }})();

    // Deltas (Σ playlists)
    (async () => {{
      try {{
        const data = await jget("/api/playlists/all-series?days=365");
        const labels = data.labels || [];
        const summed = new Array(labels.length).fill(0);
        for (const s of (data.series || [])) {{
          const vals = s.values || [];
          for (let i = 1; i < vals.length; i++) {{
            summed[i] += (vals[i] - vals[i-1]);
          }}
        }}
        const ctx = document.getElementById("playlistsDeltas");
        if (ctx) {{
          new Chart(ctx, {{
            type: "bar",
            data: {{ labels, datasets: [{{ label:"Daily follower delta (Σ playlists)", data: summed, borderWidth:1 }}] }},
            options: {{
              responsive: true,
              plugins: {{ legend: {{ display:true }}, tooltip: {{ enabled:true }} }},
              scales: {{
                x: {{ grid: {{ color: "rgba(255,255,255,0.05)" }} }},
                y: {{ grid: {{ color: "rgba(255,255,255,0.05)" }}, beginAtZero: true }}
              }}
            }}
          }});
        }}
      }} catch(e) {{ console.error("Deltas chart error:", e); }}
    }})();

    // Catalogue (Airtable, excluding 'External' distributor)
    (async () => {{
      try {{
        const url = "/api/catalogue/size-series";
        const data = await jget(url);
        const ctx = document.getElementById("catalogueChart");
        if (ctx) {{
          new Chart(ctx, {{
            type: "line",
            data: {{
              labels: data.labels || [],
              datasets: [{{ label: "Total tracks in catalogue", data: data.values || [], borderWidth: 2, fill: false, tension: 0.25 }}]
            }},
            options: {{
              responsive: true,
              plugins: {{ legend: {{ display: true }}, tooltip: {{ enabled: true }} }},
              scales: {{
                x: {{ grid: {{ color: "rgba(255,255,255,0.05)" }} }},
                y: {{ grid: {{ color: "rgba(255,255,255,0.05)" }}, beginAtZero: true }}
              }}
            }}
          }});
        }}
        const meta = document.getElementById("catalogueMeta");
        if (meta) {{
          const total = (data.values && data.values.length) ? data.values[data.values.length-1] : 0;
          const minD = data.min_date || "";
          const maxD = data.max_date || "";
          meta.textContent = `Total: ${{total}} • Range: ${{minD}} → ${{maxD}} • Records counted: ${{data.count||0}} (excl. distributor='{AIRTABLE_EXCLUDED_DISTRIBUTOR}')`;
        }}
      }} catch(e) {{
        console.error("Catalogue chart error:", e);
        const meta = document.getElementById("catalogueMeta");
        if (meta) meta.textContent = "Unable to load catalogue size. Check Airtable config.";
      }}
    }})();
  </script>
</body>
</html>
    """
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
        SELECT t.isrc, t.title, t.artist,
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
    q = """ ... """  # (unchanged SQL query)
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
    q = """ ... """  # (unchanged SQL query)
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
    days = _clamp_days(request.args.get("days"), 365)
    q = """ ... """  # (unchanged SQL query)
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

# ── API: artists ──────────────────────────────────────────────────────────────
@app.get("/api/artists/top-share")
def api_artists_top_share():
    q_latest = """ SELECT MAX(stream_date) AS d FROM streams WHERE platform='spotify' """
    r = _q(q_latest)
    latest = r[0]["d"] if r and r[0]["d"] else None
    day = request.args.get("date") or (latest.isoformat() if latest else None)
    if not day:
        return jsonify({"date": None, "labels": [], "values": [], "shares": []})
    q = """ ... """  # (unchanged SQL query)
    rows = _q(q, (day,))
    total = sum(int(r["v"]) for r in rows) or 1
    labels = [(r["artist"] or "") for r in rows]
    values = [int(r["v"]) for r in rows]
    shares = [round(v * 100.0 / total, 2) for v in values]
    return jsonify({"date": day, "labels": labels, "values": values, "shares": shares})

# ── NEW API: Catalogue size series (Airtable, distributor != External) ────────
@app.get("/api/catalogue/size-series")
def api_catalogue_size_series():
    data = _fetch_airtable_release_dates()
    if not data.get("ok"):
        return jsonify({"labels": [], "values": [], "error": data.get("error")}), 200

    labels = data["labels"] or []
    values = data["values"] or []

    days = request.args.get("days")
    if days:
        try:
            n = _clamp_days(days, default=len(labels) or 1, min_d=1, max_d=36500)
            labels = labels[-n:]
            values = values[-n:]
        except Exception:
            pass

    out = {
        "labels": labels,
        "values": values,
        "min_date": data.get("min_date").isoformat() if data.get("min_date") else None,
        "max_date": data.get("max_date").isoformat() if data.get("max_date") else None,
        "count": data.get("count", 0),
    }
    return jsonify(out)

# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    db_ok = False
    ssl_state = "unknown"
    try:
        _q("SELECT NOW()")
        db_ok = True
        ssl = _q("SHOW ssl")
        ssl_state = (ssl[0]["ssl"] if ssl else "unknown")
    except Exception as e:
        db_err = str(e)
    else:
        db_err = None

    at = _fetch_airtable_release_dates() if _airtable_enabled() else {"ok": False, "error": "Airtable not configured"}

    return {
        "ok": bool(db_ok and (at.get("ok") or not _airtable_enabled())),
        "db": {"ok": db_ok, "ssl": ssl_state, "error": db_err},
        "airtable": {"ok": at.get("ok"), "count": at.get("count"), "error": at.get("error")},
    }

# ── Entrypoint ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, threaded=True)
