const express = require("express");
const fs = require("fs");
const path = require("path");
const bodyParser = require("body-parser");
const fetch = require("node-fetch").default;
const scdl = require("soundcloud-downloader").default;

const ffmpegPath = require("ffmpeg-static");
const ffmpeg = require("fluent-ffmpeg");
ffmpeg.setFfmpegPath(ffmpegPath);

const mm = require("music-metadata");
require("dotenv").config();

// ----------------------------------
// Config
// ----------------------------------
const app = express();
app.use(bodyParser.json());

const PORT = process.env.PORT || 3000; // Railway sets PORT
const DOWNLOAD_DIR = process.env.TRACKS_DIR || path.join(__dirname, "tracks");
const BASE_ID = process.env.AIRTABLE_BASE_ID;
const TABLE = process.env.AIRTABLE_TABLE_NAME;
const HOST = (process.env.HOST_URL || "").trim();
const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY;
const CLIENT_ID = process.env.SOUNDCLOUD_CLIENT_ID;

// Polling for rehost
const CLEANUP_POLL_SECONDS = Number(process.env.CLEANUP_POLL_SECONDS || 180);
const CLEANUP_POLL_INTERVAL = Number(process.env.CLEANUP_POLL_INTERVAL || 10);

if (!fs.existsSync(DOWNLOAD_DIR)) fs.mkdirSync(DOWNLOAD_DIR, { recursive: true });

// prevent duplicate concurrent work per record
const activeJobs = new Set();

// ----------------------------------
// Helpers
// ----------------------------------
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
function ensureHttpsUrl(u) {
  return /^https?:\/\//i.test(u) ? u : `https://${u}`;
}
async function headOk(url) {
  try { const r = await fetch(url, { method: "HEAD" }); return r.ok; } catch { return false; }
}

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

// Normalize MP3 headers (ensure duration shows up consistently)
async function normalizeMp3Container(inputPath) {
  const tempOut = inputPath.replace(/\.mp3$/i, ".fixed.mp3");

  // Try remux (no quality loss) with Xing/VBR header
  const tryRemux = () =>
    new Promise((resolve, reject) => {
      ffmpeg(inputPath)
        .outputOptions(["-c:a", "copy", "-write_xing", "1"])
        .on("end", resolve)
        .on("error", reject)
        .save(tempOut);
    });

  // Fallback: re-encode to a clean container if remux fails
  const tryReencode = () =>
    new Promise((resolve, reject) => {
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

  try {
    await tryRemux();
  } catch {
    await tryReencode();
  }
  fs.renameSync(tempOut, inputPath);
}

async function ensureGoodMp3(filepath) {
  let duration = 0;
  try {
    const meta = await mm.parseFile(filepath);
    duration = meta?.format?.duration || 0;
  } catch { /* ignore */ }

  console.log(`🧭 Parsed duration before normalize: ${duration ? duration.toFixed(2) + "s" : "unknown"}`);
  if (duration && duration > 5) return; // already looks fine

  console.log("🔧 Normalizing MP3 container …");
  await normalizeMp3Container(filepath);

  try {
    const meta2 = await mm.parseFile(filepath);
    console.log(`✅ Normalized. New duration: ${meta2?.format?.duration ? meta2.format.duration.toFixed(2) + "s" : "unknown"}`);
  } catch {
    console.log("✅ Normalized (metadata parse unavailable)");
  }
}

// ----------------------------------
// Health & debug
// ----------------------------------
app.get("/health", (_req, res) => res.json({ ok: true, tracksDir: DOWNLOAD_DIR }));
app.get("/debug/files", (_req, res) => {
  try {
    const list = fs.readdirSync(DOWNLOAD_DIR).filter((f) => f.endsWith(".mp3"));
    res.json({ files: list });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

// Access logging for fetchers
app.use((req, _res, next) => {
  if (req.path.startsWith("/tracks/") || req.path.startsWith("/dl/")) {
    console.log(`📥 ${req.method} ${req.path} UA=${req.get("user-agent") || "?"}`);
  }
  next();
});

// Human-facing stream (inline)
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

// Airtable ingestion route (download semantics)
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

// ----------------------------------
// Webhook
// ----------------------------------
app.post("/webhook", async (req, res) => {
  const { record_id, soundcloud_url } = req.body || {};
  if (!record_id || !soundcloud_url) {
    return res.status(400).json({ error: "Missing record_id or soundcloud_url" });
  }
  if (activeJobs.has(record_id)) {
    return res.status(202).json({ queued: true, message: "Job already in progress for this record." });
  }
  activeJobs.add(record_id);

  const baseHost = resolveHostUrl(req);
  const filename = `${record_id}.mp3`;
  const filepath = path.join(DOWNLOAD_DIR, filename);

  // Use the /dl route for Airtable ingestion (looks like a real file download)
  const publicUrl = ensureHttpsUrl(`${baseHost}/dl/${encodeURIComponent(filename)}`);

  try {
    console.log("🌐 Base host:", baseHost);
    console.log("🔗 Computed publicUrl:", publicUrl);
    console.log("🎧 Download start:", soundcloud_url);

    // Download → disk
    const stream = await scdl.download(soundcloud_url, CLIENT_ID);
    await new Promise((resolve, reject) => {
      const out = fs.createWriteStream(filepath);
      stream.on("error", reject);
      out.on("error", reject);
      out.on("finish", resolve);
      stream.pipe(out);
    });

    const size = fs.statSync(filepath).size;
    console.log(`✅ Downloaded: ${filepath} (${size} bytes)`);

    // Normalize MP3 so duration is visible everywhere
    await ensureGoodMp3(filepath);

    // ensure URL is reachable before sending to Airtable
    for (let i = 0; i < 5; i++) {
      const ok = await headOk(publicUrl);
      console.log(`🧪 HEAD ${publicUrl} -> ${ok ? "OK" : "FAIL"}`);
      if (ok) break;
      await wait(500 * (i + 1));
    }

    // PATCH Airtable
    const airtableUrl = airtableRecordUrl(record_id);
    console.log("🔗 Sending to Airtable:", publicUrl);
    const patchResp = await fetch(airtableUrl, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${AIRTABLE_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        fields: { "Raw Track Audio File": [{ url: publicUrl, filename }] }
      }),
    });
    const patchJson = await patchResp.json();
    console.log("🧾 Airtable PATCH response:", patchJson);

    if (!patchResp.ok) {
      console.error("❌ Airtable update failed:", patchJson);
      return res.status(500).json({ error: "Airtable update failed", details: patchJson, sent_url: publicUrl });
    }

    const returnedAtt = patchJson.fields?.["Raw Track Audio File"]?.[0] || null;
    console.log("📦 Airtable returned attachment:", returnedAtt);

    // respond to caller with debug
    res.json({
      success: true,
      message: "File processed and pushed to Airtable.",
      sent_url: publicUrl,
      airtable_returned_url: returnedAtt?.url || null,
      airtable_returned: returnedAtt,
    });

    // poll for rehost → cleanup
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
    console.error("❌ SoundCloud download failed:", err);
    return res.status(500).json({ error: "SoundCloud download failed", details: String(err?.message || err) });
  } finally {
    activeJobs.delete(record_id);
  }
});

app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
