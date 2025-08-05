// index.js
const express = require("express");
const fs = require("fs");
const path = require("path");
const bodyParser = require("body-parser");
const fetch = require("node-fetch").default;
let scdlFactory;
try { scdlFactory = require("@snwfdhmp/soundcloud-downloader"); } catch { scdlFactory = require("soundcloud-downloader"); }
const createSCDL = scdlFactory.create || ((opts) => (scdlFactory.default ? scdlFactory.default : scdlFactory));
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

// ACRCloud
const ACR_HOST = process.env.ACR_HOST;
const ACR_ACCESS_KEY = process.env.ACR_ACCESS_KEY;
const ACR_ACCESS_SECRET = process.env.ACR_ACCESS_SECRET;

// Cleanup/poll
const CLEANUP_POLL_SECONDS = Number(process.env.CLEANUP_POLL_SECONDS || 180);
const CLEANUP_POLL_INTERVAL = Number(process.env.CLEANUP_POLL_INTERVAL || 10);

// ACR scan config
const ACR_RETRIES = Number(process.env.ACR_RETRIES || 2);
const ACR_BACKOFF_MS = Number(process.env.ACR_BACKOFF_MS || 400);

// SCDL client-id caching
const SAVE_CLIENT_ID = String(process.env.SAVE_CLIENT_ID || "true");
const SCDL_CLIENT_ID_FILE = process.env.SCDL_CLIENT_ID_FILE || path.join(DOWNLOAD_DIR, "client_id.json");
const SOUNDCLOUD_CLIENT_ID = process.env.SOUNDCLOUD_CLIENT_ID || undefined;

// Ensure dir
if (!fs.existsSync(DOWNLOAD_DIR)) fs.mkdirSync(DOWNLOAD_DIR, { recursive: true });

// prevent duplicate concurrent work per record
const activeJobs = new Set();

// Create SCDL instance (auto-discover client_id)
const scdl = createSCDL({
  clientID: SOUNDCLOUD_CLIENT_ID,
  saveClientID: SAVE_CLIENT_ID.toLowerCase() === "true",
  filePath: SCDL_CLIENT_ID_FILE
});

// ------------ Helpers ------------
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

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

async function fetchAirtableRecord(recordId, opts = {}) {
  const baseId = opts.baseId || BASE_ID;
  const tableName = opts.tableName || TABLE;
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}/${recordId}`;
  const r = await fetch(url, { headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` } });
  if (!r.ok) throw new Error(`Airtable GET ${r.status}`);
  return r.json();
}
const airtableRecordUrl = (recordId, opts = {}) =>
  `https://api.airtable.com/v0/${opts.baseId || BASE_ID}/${encodeURIComponent(opts.tableName || TABLE)}/${recordId}`;

async function streamToFile(readable, destPath) {
  await new Promise((resolve, reject) => {
    const ws = fs.createWriteStream(destPath);
    readable.pipe(ws);
    readable.on("error", reject);
    ws.on("finish", resolve);
    ws.on("error", reject);
  });
}
async function httpToFile(url, destPath) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`Download ${r.status}`);
  await streamToFile(r.body, destPath);
}
function cleanPermalink(u) {
  try { const url = new URL(u); url.search = ""; return url.toString(); } catch { return u; }
}
function hostOf(u) { try { return new URL(u).host?.toLowerCase() || ""; } catch { return ""; } }

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

// ------------ ACR (low-level) ------------
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

// ------------ ACR (window sweep) ------------
const ACR_WINDOW_SECONDS_FIXED = 15; // 15s per window
function selectScanWindows(durationSec) {
  const windows = [];
  windows.push({ label: "Intro", start: 0 });
  if (durationSec > 40) windows.push({ label: "Quarter", start: Math.floor(durationSec / 4) });
  if (durationSec > 60) windows.push({ label: "Middle", start: 30 });
  const outroStart = Math.max(durationSec - 15, 0);
  windows.push({ label: "Outro", start: outroStart });

  const seen = new Set();
  return windows
    .filter(w => { const k = `${w.label}:${w.start}`; if (seen.has(k)) return false; seen.add(k); return true; })
    .sort((a, b) => a.start - b.start);
}
async function identifySegment(filepath, startSec, label) {
  for (let attempt = 0; attempt <= ACR_RETRIES; attempt++) {
    try {
      const buf = await cutSegmentToBuffer(filepath, startSec, ACR_WINDOW_SECONDS_FIXED);
      const res = await acrIdentifyBuffer(buf);

      if (res?.status?.code === 0 && Array.isArray(res?.metadata?.music) && res.metadata.music.length) {
        const best = res.metadata.music[0];
        const score = Number(best.score ?? 100);
        const title = best.title || "Unknown";
        const artist = (best.artists && best.artists[0]?.name) || "Unknown";
        const line = `✅ [${label}] ${title} – ${artist}  [Score ${score}]`;
        return { match: true, line, label, start: startSec, end: startSec + ACR_WINDOW_SECONDS_FIXED, score };
      }

      const line = `❌ [${label}] No match`;
      return { match: false, line, label, start: startSec, end: startSec + ACR_WINDOW_SECONDS_FIXED, score: null };
    } catch (e) {
      if (attempt < ACR_RETRIES) await wait(ACR_BACKOFF_MS * (attempt + 1));
      else {
        const line = `❌ [${label}] No match`;
        return { match: false, line, label, start: startSec, end: startSec + ACR_WINDOW_SECONDS_FIXED, score: null };
      }
    }
  }
}
async function runAcrPythonGuiStyle(filepath) {
  if (!ACR_HOST || !ACR_ACCESS_KEY || !ACR_ACCESS_SECRET) {
    console.log("ℹ️ ACR credentials not set; skipping ACR scan.");
    return null;
  }
  let dur = 0;
  try {
    const meta = await mm.parseFile(filepath);
    dur = Math.max(0, Math.round(meta?.format?.duration || 0));
  } catch {}

  if (!dur) return "⚠️  Could not read duration, skipping.";

  const windows = selectScanWindows(dur);
  const results = [];
  for (const w of windows) {
    const r = await identifySegment(filepath, w.start, w.label);
    console.log(r.line);
    results.push(r);
    await wait(150);
  }

  const anyDetected = results.some(r => r.match);
  const header = anyDetected ? "⛔ Fingerprint detected" : "✅ No matches detected";
  const lines = results.map(r => r.line);
  return [header, ...lines].join("\n");
}

// ------------ SoundCloud download helpers ------------
function readCachedClientId() {
  if (SOUNDCLOUD_CLIENT_ID) return SOUNDCLOUD_CLIENT_ID;
  try {
    if (fs.existsSync(SCDL_CLIENT_ID_FILE)) {
      const j = JSON.parse(fs.readFileSync(SCDL_CLIENT_ID_FILE, "utf8"));
      if (j && (j.client_id || j.clientId)) return j.client_id || j.clientId;
    }
  } catch {}
  return null;
}
async function downloadViaSCDL(permalinkUrl, destPath) {
  const clean = cleanPermalink(permalinkUrl);
  const stream = await scdl.download(clean);
  await streamToFile(stream, destPath);
  return "scdl";
}
async function resolveWithClientId(permalinkUrl, clientId) {
  const base = "https://api-v2.soundcloud.com/resolve";
  const url = `${base}?url=${encodeURIComponent(cleanPermalink(permalinkUrl))}&client_id=${encodeURIComponent(clientId)}&app_locale=en`;
  const r = await fetch(url, { headers: { "accept": "application/json" } });
  if (!r.ok) throw new Error(`resolve ${r.status}`);
  return r.json();
}
async function getMediaUrl(transcodingUrl, clientId) {
  const u = transcodingUrl.startsWith("http") ? transcodingUrl : `https://api-v2.soundcloud.com${transcodingUrl}`;
  const r = await fetch(`${u}${u.includes("?") ? "&" : "?"}client_id=${encodeURIComponent(clientId)}`, {
    headers: { "accept": "application/json" }
  });
  if (!r.ok) throw new Error(`transcoding ${r.status}`);
  const j = await r.json();
  if (!j || !j.url) throw new Error("no media url");
  return j.url;
}
async function downloadViaApiV2(permalinkUrl, destPath, clientId) {
  const track = await resolveWithClientId(permalinkUrl, clientId);
  const trans = (track?.media?.transcodings) || [];
  const progressive = trans.find(t =>
    /progressive/i.test(t.preset || "") || /progressive/i.test(t.format?.protocol || "")
  );
  const hls = trans.find(t => /hls/i.test(t.format?.protocol || ""));

  if (progressive) {
    const mediaUrl = await getMediaUrl(progressive.url, clientId);
    await httpToFile(mediaUrl, destPath);
    return "api-v2:mp3";
  }
  if (hls) {
    const mediaUrl = await getMediaUrl(hls.url, clientId);
    await new Promise((resolve, reject) => {
      ffmpeg(mediaUrl)
        .audioCodec("libmp3lame")
        .audioBitrate("192k")
        .format("mp3")
        .on("end", resolve)
        .on("error", reject)
        .save(destPath);
    });
    return "api-v2:hls->mp3";
  }
  throw new Error("no playable transcodings");
}
async function downloadTrackResilient(permalinkUrl, destPath) {
  let lastErr;

  for (let i = 1; i <= 2; i++) {
    try {
      const how = await downloadViaSCDL(permalinkUrl, destPath);
      return how;
    } catch (e) {
      lastErr = e;
      console.log(`⚠️ SCDL attempt ${i} failed: ${e?.message || e}`);
      await wait(400 * i);
    }
  }

  for (let i = 1; i <= 2; i++) {
    const cid = readCachedClientId();
    if (!cid) break;
    try {
      const how = await downloadViaApiV2(permalinkUrl, destPath, cid);
      console.log(`ℹ️ api-v2 fallback succeeded with client_id=${cid.slice(0,6)}…`);
      return how;
    } catch (e) {
      lastErr = e;
      console.log(`⚠️ api-v2 attempt ${i} failed: ${e?.message || e}`);
      try { if (fs.existsSync(SCDL_CLIENT_ID_FILE)) fs.unlinkSync(SCDL_CLIENT_ID_FILE); } catch {}
      await wait(500 * i);
    }
  }

  throw lastErr || new Error("SoundCloud download failed");
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

// ------------ POST /webhook (SC → attach → optional ACR via skip_acr) ------------
app.post("/webhook", async (req, res) => {
  const { record_id, soundcloud_url, skip_acr = false } = req.body || {};
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

    const how = await downloadTrackResilient(soundcloud_url, filepath);
    console.log(`✅ Downloaded via ${how}: ${filepath} (${fs.statSync(filepath).size} bytes)`);

    await ensureGoodMp3(filepath);

    let acrReport = null;
    if (!skip_acr) {
      acrReport = await runAcrPythonGuiStyle(filepath);
    }

    for (let i = 0; i < 5; i++) {
      const ok = await headOk(publicUrl);
      console.log(`🧪 HEAD ${publicUrl} -> ${ok ? "OK" : "FAIL"}`);
      if (ok) break;
      await wait(500 * (i + 1));
    }

    const airtableUrl = airtableRecordUrl(record_id);
    const fields = { "Raw Track Audio File": [{ url: publicUrl, filename }] };
    if (!skip_acr && acrReport) fields["ACR Report"] = acrReport;

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
      message: skip_acr
        ? "File processed and pushed to Airtable (ACR skipped)."
        : "File processed, ACR scanned, and pushed to Airtable.",
      sent_url: publicUrl,
      airtable_returned_url: returnedAtt?.url || null,
      airtable_returned: returnedAtt,
      acr_report: skip_acr ? null : (acrReport || null)
    });

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
    console.log("❌ SoundCloud download/attach failed:", err?.message || err);
    res.status(500).json({ error: "SoundCloud download/attach failed", details: String(err?.message || err) });
  } finally {
    activeJobs.delete(record_id);
  }
});

// ------------ POST /acr (run ACR on existing attachment; recover if stale) ------------
app.post("/acr", async (req, res) => {
  const { record_id, attachment_url, force = false, table_name, base_id } = req.body || {};
  if (!record_id) return res.status(400).json({ error: "Missing record_id" });
  if (activeJobs.has(record_id)) return res.status(202).json({ queued: true });
  activeJobs.add(record_id);

  const opts = { baseId: base_id || BASE_ID, tableName: table_name || TABLE };

  let workFile = path.join(DOWNLOAD_DIR, `acr-${record_id}.mp3`);
  const cleanupPaths = [];
  let rehydratedFromSource = false;
  let usedUrl = null;

  try {
    const rec = await fetchAirtableRecord(record_id, opts);
    const fields = rec.fields || {};
    if (!force && fields["ACR Report"]) {
      activeJobs.delete(record_id);
      return res.json({ success: true, skipped: true, reason: "Report already present" });
    }

    // Prefer the passed URL; else use field
    let attUrl = attachment_url ||
      (Array.isArray(fields["Raw Track Audio File"]) && fields["Raw Track Audio File"][0]?.url);
    if (!attUrl) return res.status(400).json({ error: "No attachment URL on record" });

    // Robust own-host detection by hostname (env host OR current request host)
    const envHost = hostOf(HOST);
    const reqHost = hostOf(resolveHostUrl(req));
    const attHost = hostOf(attUrl);
    const attIsOurHost = !!attHost && (attHost === envHost || attHost === reqHost);

    // Try to download the attachment first
    try {
      usedUrl = attUrl;
      await httpToFile(attUrl, workFile);
      cleanupPaths.push(workFile);
    } catch (e) {
      console.log(`⚠️ Attachment download failed (${attUrl}): ${e?.message || e}`);
      if (attIsOurHost) {
        // Rehydrate from a known source field
        const src =
          fields["Input Track Link"] ||
          (typeof fields["Source Links"] === "string" ? fields["Source Links"].split(/\s+/)[0] : null);
        if (!src) throw new Error("Attachment dead and no source link on record");

        workFile = path.join(DOWNLOAD_DIR, `${record_id}.mp3`);
        console.log(`♻️ Rehydrating from source: ${src}`);
        await downloadTrackResilient(src, workFile);
        await ensureGoodMp3(workFile);
        cleanupPaths.push(workFile);
        rehydratedFromSource = true;
      } else {
        // Not our host → don't guess; bubble the error
        throw e;
      }
    }

    if (!rehydratedFromSource && fs.existsSync(workFile)) {
      await ensureGoodMp3(workFile);
    }

    const acrReport = await runAcrPythonGuiStyle(workFile);

    // Prepare update
    const patchUrl = airtableRecordUrl(record_id, opts);
    const update = {};
    if (typeof acrReport === "string") update["ACR Report"] = acrReport;

    // If rehydrated, reattach so Airtable hosts it (then we can clean locally)
    if (rehydratedFromSource) {
      const attachFilename = `${record_id}.mp3`;
      const baseHost = resolveHostUrl(req);
      const publicUrl = ensureHttpsUrl(`${baseHost}/dl/${encodeURIComponent(attachFilename)}`);

      // Ensure named file exists for serving
      if (!workFile.endsWith(attachFilename)) {
        const newPath = path.join(DOWNLOAD_DIR, attachFilename);
        fs.copyFileSync(workFile, newPath);
        cleanupPaths.push(newPath);
        workFile = newPath;
      }

      // Try until served
      for (let i = 0; i < 5; i++) {
        if (await headOk(publicUrl)) break;
        await wait(500 * (i + 1));
      }
      update["Raw Track Audio File"] = [{ url: publicUrl, filename: attachFilename }];

      // Patch + poll until Airtable copies to airtableusercontent → then cleanup
      const patchResp = await fetch(patchUrl, {
        method: "PATCH",
        headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}`, "Content-Type": "application/json" },
        body: JSON.stringify({ fields: update }),
      });
      const patchJson = await patchResp.json();
      if (!patchResp.ok) return res.status(500).json({ error: "Airtable update failed", details: patchJson });

      const t0 = Date.now();
      while (Date.now() - t0 < CLEANUP_POLL_SECONDS * 1000) {
        await wait(CLEANUP_POLL_INTERVAL * 1000);
        try {
          const pollResp = await fetch(patchUrl, { headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` } });
          const pollJson = await pollResp.json();
          const att = pollJson.fields?.["Raw Track Audio File"];
          const currentUrl = att?.[0]?.url || null;
          console.log("🔎 Current attachment URL:", currentUrl);
          if (currentUrl && currentUrl.includes("airtableusercontent")) {
            for (const p of cleanupPaths) { try { if (fs.existsSync(p)) fs.unlinkSync(p); } catch {} }
            console.log(`🧹 Cleaned up rehydrated file(s) for ${record_id}`);
            break;
          }
        } catch (e) {
          console.error("⚠️ Polling error:", e);
        }
      }

      return res.json({
        success: true,
        wrote_report: typeof acrReport === "string",
        acr_report: acrReport || null,
        attachment_used: usedUrl,
        rehydrated_from_source: true
      });
    }

    // No reattach needed → just patch report
    const patchResp = await fetch(patchUrl, {
      method: "PATCH",
      headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({ fields: update }),
    });
    const patchJson = await patchResp.json();
    if (!patchResp.ok) return res.status(500).json({ error: "Airtable update failed", details: patchJson });

    res.json({
      success: true,
      wrote_report: typeof acrReport === "string",
      acr_report: acrReport || null,
      attachment_used: usedUrl,
      rehydrated_from_source: false
    });
  } catch (e) {
    console.error("ACR route error:", e);
    res.status(500).json({ error: String(e?.message || e) });
  } finally {
    activeJobs.delete(record_id);
    // best-effort cleanup (in case we didn't hit the poll branch)
    try { if (fs.existsSync(workFile)) fs.unlinkSync(workFile); } catch {}
  }
});

app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
