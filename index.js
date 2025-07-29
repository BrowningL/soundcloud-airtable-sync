const express = require("express");
const fs = require("fs");
const path = require("path");
const bodyParser = require("body-parser");
const fetch = require("node-fetch").default;
const ffmpegPath = require("ffmpeg-static");
const ffmpeg = require("fluent-ffmpeg");
ffmpeg.setFfmpegPath(ffmpegPath);
const mm = require("music-metadata");
const crypto = require("crypto");
const FormData = require("form-data");
require("dotenv").config();

// ------------ Config ------------
const app = express();
app.use(bodyParser.json());

const PORT = process.env.PORT || 3000;
const DOWNLOAD_DIR = process.env.TRACKS_DIR || path.join(__dirname, "tracks");

const BASE_ID = process.env.AIRTABLE_BASE_ID;
const TABLE = process.env.AIRTABLE_TABLE_NAME;
const HOST = (process.env.HOST_URL || "").trim();
const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY;

// SoundCloud OAuth (official)
const SC_CLIENT_ID = process.env.SC_CLIENT_ID;
const SC_CLIENT_SECRET = process.env.SC_CLIENT_SECRET;

// ACRCloud
const ACR_HOST = process.env.ACR_HOST;
const ACR_ACCESS_KEY = process.env.ACR_ACCESS_KEY;
const ACR_ACCESS_SECRET = process.env.ACR_ACCESS_SECRET;

// Cleanup/poll
const CLEANUP_POLL_SECONDS = Number(process.env.CLEANUP_POLL_SECONDS || 180);
const CLEANUP_POLL_INTERVAL = Number(process.env.CLEANUP_POLL_INTERVAL || 10);

// ACR sweep
const ACR_WINDOW_SEC  = Number(process.env.ACR_WINDOW_SEC  || 12);
const ACR_STEP_SEC    = Number(process.env.ACR_STEP_SEC    || 25);
const ACR_MAX_REQ     = Number(process.env.ACR_MAX_REQUESTS|| 100); // thorough sweep
const ACR_MIN_SCORE   = Number(process.env.ACR_MIN_SCORE   || 70);
const ACR_RETRIES     = Number(process.env.ACR_RETRIES     || 2);
const ACR_BACKOFF_MS  = Number(process.env.ACR_BACKOFF_MS  || 400);

if (!fs.existsSync(DOWNLOAD_DIR)) fs.mkdirSync(DOWNLOAD_DIR, { recursive: true });
const activeJobs = new Set();

// ------------ Helpers ------------
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const airtableRecordUrl = (recordId) =>
  `https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(TABLE)}/${recordId}`;

function normalizeHost(h) {
  if (!h) return null;
  let v = h.trim();
  if (!/^https?:\/\//i.test(v)) v = `https://${v}`;
  return v.replace(/\/$/, "");
}
function resolveHostUrl(req) {
  const fromEnv = normalizeHost(HOST);
  if (fromEnv) return fromEnv;
  const proto = (req.get("x-forwarded-proto") || req.protocol || "https").split(",")[0].trim();
  const host = (req.get("x-forwarded-host") || req.get("host") || "").split(",")[0].trim();
  return normalizeHost(`${proto}://${host}`);
}
function ensureHttpsUrl(u) { return /^https?:\/\//i.test(u) ? u : `https://${u}`; }
async function headOk(url) { try { const r = await fetch(url, { method: "HEAD" }); return r.ok; } catch { return false; } }

function serveInlineHeaders(res, filename, stat) {
  res.setHeader("Content-Type", "audio/mpeg");
  res.setHeader("Content-Length", stat.size);
  res.setHeader("Accept-Ranges", "bytes");
  res.setHeader("Cache-Control", "public, max-age=3600");
  res.setHeader("Content-Disposition", `inline; filename="${filename}"`);
}
function serveDownloadHeaders(res, filename, stat) {
  res.setHeader("Content-Type", "audio/mpeg");
  res.setHeader("Content-Length", stat.size);
  res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
  res.setHeader("Cache-Control", "public, max-age=300");
}
async function fetchAirtableRecord(recordId) {
  const url = `https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(TABLE)}/${recordId}`;
  const r = await fetch(url, { headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` } });
  if (!r.ok) throw new Error(`Airtable GET ${r.status}`);
  return r.json();
}
async function downloadUrlToFile(url, destPath) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`Download ${r.status}`);
  await new Promise((resolve, reject) => {
    const ws = fs.createWriteStream(destPath);
    r.body.pipe(ws);
    r.body.on("error", reject);
    ws.on("finish", resolve);
    ws.on("error", reject);
  });
}

// ------------ MP3 normalization ------------
async function normalizeMp3Container(inputPath) {
  const tempOut = inputPath.replace(/\.mp3$/i, ".fixed.mp3");
  await new Promise((resolve, reject) => {
    ffmpeg(inputPath)
      .audioCodec("libmp3lame")
      .audioBitrate("192k")
      .audioFrequency(44100)
      .noVideo()
      .outputOptions(["-write_xing", "1"])
      .on("end", resolve)
      .on("error", reject)
      .save(tempOut);
  });
  fs.renameSync(tempOut, inputPath);
}
async function ensureGoodMp3(filepath) {
  await normalizeMp3Container(filepath);
  try {
    const meta = await mm.parseFile(filepath);
    console.log(`✅ Normalized MP3, duration≈${meta?.format?.duration ? meta.format.duration.toFixed(2)+"s" : "unknown"}`);
  } catch {}
}

// ------------ ACR (distributor-style sweep) ------------
async function acrIdentifyBuffer(sampleBuf) {
  const http_method = "POST";
  const http_uri = "/v1/identify";
  const data_type = "audio";
  const signature_version = "1";
  const timestamp = Math.floor(Date.now() / 1000);

  const stringToSign = [http_method, http_uri, ACR_ACCESS_KEY, data_type, signature_version, timestamp].join("\n");
  const signature = crypto.createHmac("sha1", ACR_ACCESS_SECRET).update(stringToSign).digest("base64");

  const form = new FormData();
  form.append("access_key", ACR_ACCESS_KEY);
  form.append("sample", sampleBuf, { filename: "sample.mp3", contentType: "audio/mpeg" });
  form.append("sample_bytes", String(sampleBuf.length));
  form.append("data_type", data_type);
  form.append("signature_version", signature_version);
  form.append("signature", signature);
  form.append("timestamp", String(timestamp));

  const url = `https://${ACR_HOST}${http_uri}`;
  const resp = await fetch(url, { method: "POST", body: form, headers: form.getHeaders() });
  if (!resp.ok) throw new Error(`ACR HTTP ${resp.status}`);
  return resp.json();
}
async function cutSegmentToBuffer(filepath, startSec, durSec) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    const out = ffmpeg(filepath)
      .setStartTime(Math.max(0, startSec))
      .duration(durSec)
      .audioCodec("libmp3lame")
      .audioBitrate("192k")
      .format("mp3")
      .on("error", reject)
      .on("end", () => resolve(Buffer.concat(chunks)))
      .pipe();
    out.on("data", (d) => chunks.push(d));
  });
}
function makeSweepPositions(durationSec) {
  if (!durationSec || durationSec < 10) return [0];
  const positions = new Set([0]);
  const step = Math.max(ACR_STEP_SEC, ACR_WINDOW_SEC + 1);
  while (positions.size < ACR_MAX_REQ - 1) {
    const next = Math.min(durationSec - ACR_WINDOW_SEC, positions.size * step);
    if (next <= 0 || next >= durationSec - 1) break;
    positions.add(Math.floor(next));
    if (next >= durationSec - ACR_WINDOW_SEC - 1) break;
  }
  positions.add(Math.max(0, durationSec - ACR_WINDOW_SEC));
  return Array.from(positions).sort((a, b) => a - b);
}
function aggregateMarks(marks) {
  const key = (m) => `${m.title || "?"}__${m.artist || "?"}`;
  const groups = new Map();
  for (const m of marks) {
    if (!m.match) continue;
    const prev = groups.get(k = key(m)) || { title: m.title, artist: m.artist, maxScore: 0, hits: 0 };
    prev.maxScore = Math.max(prev.maxScore, m.score || 0);
    prev.hits += 1;
    groups.set(k, prev);
  }
  const best = Array.from(groups.values()).sort((a, b) => (b.hits - a.hits) || (b.maxScore - a.maxScore))[0] || null;
  return { best, groups };
}
function formatReport(marks, durationSec) {
  const hits = marks.filter(m => m.match);
  const header = hits.length
    ? `✖ Matches detected — ${hits.length}/${marks.length} windows (duration ~${Math.round(durationSec)}s)`
    : `✔ No matches detected — scanned ${marks.length} windows (duration ~${Math.round(durationSec)}s)`;
  const lines = [header];
  for (const m of marks) {
    lines.push(m.match
      ? `✅ [t≈${m.pos}s] ${m.title} – ${m.artist} [Score ${m.score}]`
      : `❌ [t≈${m.pos}s] No match`
    );
  }
  const { best } = aggregateMarks(marks);
  if (best) lines.push(`\nTop candidate: ${best.title} – ${best.artist} (hits ${best.hits}, max score ${best.maxScore})`);
  return lines.join("\n");
}
async function identifyWindow(filepath, pos) {
  for (let attempt = 0; attempt <= ACR_RETRIES; attempt++) {
    try {
      const buf = await cutSegmentToBuffer(filepath, pos, ACR_WINDOW_SEC);
      const res = await acrIdentifyBuffer(buf);
      if (res?.status?.code === 0 && Array.isArray(res?.metadata?.music) && res.metadata.music.length) {
        const best = res.metadata.music[0];
        const score = Number(best.score ?? 0);
        if (score >= ACR_MIN_SCORE) {
          return { pos, match: true, title: best.title || "Unknown",
                   artist: (best.artists && best.artists[0]?.name) || "Unknown", score };
        }
      }
      return { pos, match: false };
    } catch (e) {
      if (attempt < ACR_RETRIES) await wait(ACR_BACKOFF_MS * (attempt + 1));
      else return { pos, match: false };
    }
  }
}
async function runAcrDistributorStyle(filepath) {
  if (!ACR_HOST || !ACR_ACCESS_KEY || !ACR_ACCESS_SECRET) {
    console.log("ℹ️ ACR credentials not set; skipping ACR scan.");
    return null;
  }
  let dur = 0;
  try { const meta = await mm.parseFile(filepath); dur = Math.round(meta?.format?.duration || 0); } catch {}
  if (!dur) return "✖ Could not read duration";
  const positions = makeSweepPositions(dur);
  console.log("🧭 ACR sweep positions (sec):", positions);
  const marks = [];
  let consecutive = 0;
  for (const pos of positions) {
    const m = await identifyWindow(filepath, pos);
    marks.push(m);
    console.log(m.match ? `🔎 ACR t≈${pos}s: ${m.title} – ${m.artist} [${m.score}]`
                        : `🔎 ACR t≈${pos}s: No match`);
    consecutive++;
    if (consecutive % 5 === 0) await wait(800); // gentle pacing every 5 calls
  }
  return formatReport(marks, dur);
}

// ------------ SoundCloud (official API) ------------
let scTokenCache = { access_token: null, refresh_token: null, expires_at: 0 };

async function getSoundCloudAccessToken() {
  const now = Math.floor(Date.now() / 1000);
  if (scTokenCache.access_token && now < scTokenCache.expires_at - 60) {
    return scTokenCache.access_token;
  }
  if (scTokenCache.refresh_token) {
    // Try refresh once
    try {
      const r = await fetch("https://secure.soundcloud.com/oauth/token", {
        method: "POST",
        headers: { "accept": "application/json; charset=utf-8",
                   "content-type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          grant_type: "refresh_token",
          client_id: SC_CLIENT_ID,
          client_secret: SC_CLIENT_SECRET,
          refresh_token: scTokenCache.refresh_token,
        })
      });
      if (r.ok) {
        const j = await r.json();
        scTokenCache.access_token = j.access_token;
        scTokenCache.refresh_token = j.refresh_token || scTokenCache.refresh_token;
        scTokenCache.expires_at = Math.floor(Date.now()/1000) + Math.floor(j.expires_in || 3600);
        return scTokenCache.access_token;
      }
    } catch {}
  }
  // Client Credentials
  const basic = Buffer.from(`${SC_CLIENT_ID}:${SC_CLIENT_SECRET}`).toString("base64");
  const resp = await fetch("https://secure.soundcloud.com/oauth/token", {
    method: "POST",
    headers: {
      "accept": "application/json; charset=utf-8",
      "content-type": "application/x-www-form-urlencoded",
      "authorization": `Basic ${basic}`
    },
    body: new URLSearchParams({ grant_type: "client_credentials" })
  });
  if (!resp.ok) throw new Error(`SoundCloud token HTTP ${resp.status}`);
  const json = await resp.json();
  scTokenCache.access_token = json.access_token;
  scTokenCache.refresh_token = json.refresh_token || null;
  scTokenCache.expires_at = Math.floor(Date.now()/1000) + Math.floor(json.expires_in || 3600);
  return scTokenCache.access_token;
}

async function scFetch(pathname, init={}) {
  const token = await getSoundCloudAccessToken();
  const headers = Object.assign({}, init.headers, {
    "accept": "application/json; charset=utf-8",
    "authorization": `OAuth ${token}` // per docs
  });
  const url = /^https?:/.test(pathname) ? pathname : `https://api.soundcloud.com${pathname}`;
  const r = await fetch(url, Object.assign({}, init, { headers }));
  if (!r.ok) throw new Error(`SC ${pathname} -> ${r.status}`);
  return r.json();
}

async function resolvePermalink(permalinkUrl) {
  return scFetch(`/resolve?url=${encodeURIComponent(permalinkUrl)}`);
}

async function getStreamUrlForTrack(track) {
  // Docs: GET /tracks/:id/stream => set of links with available transcodings
  const id = track.id || track.urn || track.permalink || null;
  if (!track.id) {
    // If resolve returned URN only, fetch by URN → reference field with id
    // Best effort: /resolve already returns id for public tracks; if not, try to pull by URN
  }
  const streams = await scFetch(`/tracks/${track.id}/stream`);
  // Prefer progressive MP3; else HLS
  const mp3 = streams?.transcodings?.find(t => /progressive/i.test(t.preset) || /progressive/i.test(t.format?.protocol || ""));
  const hls = streams?.transcodings?.find(t => /hls/i.test(t.format?.protocol || ""));
  return { mp3, hls };
}

async function downloadTrackToFile(permalinkUrl, destPath) {
  const track = await resolvePermalink(permalinkUrl);
  const { mp3, hls } = await getStreamUrlForTrack(track);

  // Each transcoding has a URL to fetch signed playback URL(s)
  async function fetchPlaybackUrl(transcoding) {
    const token = await getSoundCloudAccessToken();
    const r = await fetch(transcoding.url.startsWith("http") ? transcoding.url : `https://api.soundcloud.com${transcoding.url}`, {
      headers: { "authorization": `OAuth ${token}`, "accept": "*/*" }
    });
    if (!r.ok) throw new Error(`SC stream URL ${r.status}`);
    const j = await r.json();
    return j.url; // actual media URL (mp3 or m3u8)
  }

  if (mp3) {
    const mediaUrl = await fetchPlaybackUrl(mp3);
    await downloadUrlToFile(mediaUrl, destPath);
    return "mp3";
  }
  if (hls) {
    const mediaUrl = await fetchPlaybackUrl(hls);
    // Pull HLS and transcode/remux to MP3
    await new Promise((resolve, reject) => {
      ffmpeg(mediaUrl)
        .audioCodec("libmp3lame")
        .audioBitrate("192k")
        .format("mp3")
        .on("end", resolve)
        .on("error", reject)
        .save(destPath);
    });
    return "hls->mp3";
  }
  throw new Error("No playable transcodings available");
}

// ------------ Health & static ------------
app.get("/health", (_req, res) => res.json({ ok: true, tracksDir: DOWNLOAD_DIR }));

app.get("/tracks/:filename", (req, res) => {
  const fpath = path.join(DOWNLOAD_DIR, req.params.filename);
  if (!fs.existsSync(fpath)) return res.status(404).send("Not found");
  const stat = fs.statSync(fpath);
  serveInlineHeaders(res, req.params.filename, stat);
  res.sendFile(fpath);
});
app.head("/tracks/:filename", (req, res) => {
  const fpath = path.join(DOWNLOAD_DIR, req.params.filename);
  if (!fs.existsSync(fpath)) return res.sendStatus(404);
  const stat = fs.statSync(fpath);
  serveInlineHeaders(res, req.params.filename, stat);
  res.sendStatus(200);
});

app.get("/dl/:filename", (req, res) => {
  const fpath = path.join(DOWNLOAD_DIR, req.params.filename);
  if (!fs.existsSync(fpath)) return res.status(404).send("Not found");
  const stat = fs.statSync(fpath);
  serveDownloadHeaders(res, req.params.filename, stat);
  res.sendFile(fpath);
});
app.head("/dl/:filename", (req, res) => {
  const fpath = path.join(DOWNLOAD_DIR, req.params.filename);
  if (!fs.existsSync(fpath)) return res.sendStatus(404);
  const stat = fs.statSync(fpath);
  serveDownloadHeaders(res, req.params.filename, stat);
  res.sendStatus(200);
});

// ------------ POST /webhook (SC → attach → ACR) ------------
app.post("/webhook", async (req, res) => {
  const { record_id, soundcloud_url } = req.body || {};
  if (!record_id || !soundcloud_url) return res.status(400).json({ error: "Missing record_id or soundcloud_url" });
  if (activeJobs.has(record_id)) return res.status(202).json({ queued: true, message: "Job already in progress" });
  activeJobs.add(record_id);

  const baseHost = resolveHostUrl(req);
  const filename = `${record_id}.mp3`;
  const filepath = path.join(DOWNLOAD_DIR, filename);
  const publicUrl = ensureHttpsUrl(`${baseHost}/dl/${encodeURIComponent(filename)}`);

  try {
    console.log("🔗 publicUrl:", publicUrl);
    console.log("🎧 Download start:", soundcloud_url);

    const how = await downloadTrackToFile(soundcloud_url, filepath);
    console.log(`✅ Downloaded via ${how}: ${filepath} (${fs.statSync(filepath).size} bytes)`);

    // Normalize then ACR
    await ensureGoodMp3(filepath);
    const acrReport = await runAcrDistributorStyle(filepath);

    // Preflight URL
    for (let i = 0; i < 5; i++) {
      const ok = await headOk(publicUrl);
      console.log(`🧪 HEAD ${publicUrl} -> ${ok ? "OK" : "FAIL"}`);
      if (ok) break;
      await wait(500 * (i + 1));
    }

    // Patch Airtable
    const airtableUrl = airtableRecordUrl(record_id);
    const fields = { "Raw Track Audio File": [{ url: publicUrl, filename }] };
    if (acrReport) fields["ACR Report"] = acrReport;

    const patchResp = await fetch(airtableUrl, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${AIRTABLE_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields }),
    });
    const patchJson = await patchResp.json();
    if (!patchResp.ok) return res.status(500).json({ error: "Airtable update failed", details: patchJson });

    const returnedAtt = patchJson.fields?.["Raw Track Audio File"]?.[0] || null;
    res.json({
      success: true,
      message: "File processed, ACR scanned, and pushed to Airtable.",
      sent_url: publicUrl,
      airtable_returned_url: returnedAtt?.url || null,
      airtable_returned: returnedAtt,
      acr_report: acrReport || null
    });

    // Cleanup once Airtable rehosts
    const t0 = Date.now();
    while (Date.now() - t0 < CLEANUP_POLL_SECONDS * 1000) {
      await wait(CLEANUP_POLL_INTERVAL * 1000);
      try {
        const pollResp = await fetch(airtableUrl, { headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` } });
        const pollJson = await pollResp.json();
        const att = pollJson.fields?.["Raw Track Audio File"];
        const currentUrl = att?.[0]?.url || null;
        console.log("🔎 Current attachment URL:", currentUrl);
        if (currentUrl && currentUrl.includes("airtableusercontent")) {
          if (fs.existsSync(filepath)) fs.unlinkSync(filepath);
          console.log(`🧹 Cleaned up ${filename}`);
          break;
        }
      } catch (e) {
        console.error("⚠️ Polling error:", e);
      }
    }
  } catch (err) {
    console.error("❌ SoundCloud download/attach failed:", err?.message || err);
    res.status(500).json({ error: "SoundCloud download/attach failed", details: String(err?.message || err) });
  } finally {
    activeJobs.delete(record_id);
  }
});

// ------------ POST /acr (run ACR on existing attachment) ------------
app.post("/acr", async (req, res) => {
  const { record_id, attachment_url, force = false } = req.body || {};
  if (!record_id) return res.status(400).json({ error: "Missing record_id" });
  if (activeJobs.has(record_id)) return res.status(202).json({ queued: true });
  activeJobs.add(record_id);

  const tmpFile = path.join(DOWNLOAD_DIR, `acr-${record_id}.mp3`);
  try {
    const rec = await fetchAirtableRecord(record_id);
    const fields = rec.fields || {};
    if (!force && fields["ACR Report"]) {
      return res.json({ success: true, skipped: true, reason: "Report already present" });
    }
    const attUrl =
      attachment_url ||
      (Array.isArray(fields["Raw Track Audio File"]) && fields["Raw Track Audio File"][0]?.url);
    if (!attUrl) return res.status(400).json({ error: "No attachment URL on record" });

    await downloadUrlToFile(attUrl, tmpFile);
    await ensureGoodMp3(tmpFile);
    const acrReport = await runAcrDistributorStyle(tmpFile);

    const patchUrl = `https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(TABLE)}/${record_id}`;
    const patchResp = await fetch(patchUrl, {
      method: "PATCH",
      headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({ fields: { "ACR Report": acrReport } }),
    });
    const patchJson = await patchResp.json();
    if (!patchResp.ok) return res.status(500).json({ error: "Airtable update failed", details: patchJson });

    res.json({ success: true, wrote_report: true, acr_report: acrReport });
  } catch (e) {
    console.error("ACR route error:", e);
    res.status(500).json({ error: String(e?.message || e) });
  } finally {
    activeJobs.delete(record_id);
    if (fs.existsSync(tmpFile)) fs.unlinkSync(tmpFile);
  }
});

app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
