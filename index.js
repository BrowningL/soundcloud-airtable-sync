const express = require("express");
const fs = require("fs");
const path = require("path");
const bodyParser = require("body-parser");
const fetch = require("node-fetch").default;
const scdl = require("soundcloud-downloader").default; // v1.0.0 usage
require("dotenv").config();

// -----------------------------
// Config
// -----------------------------
const app = express();
app.use(bodyParser.json());

const PORT = process.env.PORT || 3000; // Railway provides PORT
const DOWNLOAD_DIR = process.env.TRACKS_DIR || path.join(__dirname, "tracks");
const BASE_ID = process.env.AIRTABLE_BASE_ID;
const TABLE = process.env.AIRTABLE_TABLE_NAME;
const HOST = process.env.HOST_URL; // e.g. https://soundcloud-airtable-sync-production.up.railway.app
const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY;
const CLIENT_ID = process.env.SOUNDCLOUD_CLIENT_ID;
const SAVE_CLIENT_ID = (process.env.SAVE_CLIENT_ID || "false").toLowerCase() === "true";
const CLEANUP_POLL_SECONDS = Number(process.env.CLEANUP_POLL_SECONDS || 90);
const CLEANUP_POLL_INTERVAL = Number(process.env.CLEANUP_POLL_INTERVAL || 10);

if (!fs.existsSync(DOWNLOAD_DIR)) fs.mkdirSync(DOWNLOAD_DIR, { recursive: true });

// In‑memory job lock to avoid duplicate work per record
const activeJobs = new Set();

// -----------------------------
// Helpers
// -----------------------------
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const airtableRecordUrl = (recordId) => `https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(TABLE)}/${recordId}`;

function resolveHostUrl(req) {
  // If HOST not provided, infer from request (works behind Railway proxy)
  if (HOST) return HOST.replace(/\/$/, "");
  const proto = req.get("x-forwarded-proto") || req.protocol || "https";
  const host = req.get("x-forwarded-host") || req.get("host");
  return `${proto}://${host}`;
}

async function headOk(url) {
  try {
    const resp = await fetch(url, { method: "HEAD" });
    return resp.ok;
  } catch (_) {
    return false;
  }
}

function serveFileHeaders(res, filename, stat) {
  res.setHeader("Content-Type", "audio/mpeg");
  res.setHeader("Content-Length", stat.size);
  res.setHeader("Accept-Ranges", "bytes");
  res.setHeader("Cache-Control", "public, max-age=3600");
  res.setHeader("Content-Disposition", `inline; filename="${filename}"`);
}

// -----------------------------
// Health & debug
// -----------------------------
app.get("/health", (_req, res) => res.json({ ok: true, tracksDir: DOWNLOAD_DIR }));
app.get("/debug/files", (_req, res) => {
  try {
    const list = fs.readdirSync(DOWNLOAD_DIR).filter((f) => f.endsWith(".mp3"));
    res.json({ files: list });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

// -----------------------------
// Static file serving for Airtable fetcher
// -----------------------------
app.get("/tracks/:filename", (req, res) => {
  const file = req.params.filename;
  const fpath = path.join(DOWNLOAD_DIR, file);
  if (!fs.existsSync(fpath)) return res.status(404).send("Not found");
  const stat = fs.statSync(fpath);
  serveFileHeaders(res, file, stat);
  res.sendFile(fpath);
});

// Explicit HEAD support (some fetchers preflight with HEAD)
app.head("/tracks/:filename", (req, res) => {
  const file = req.params.filename;
  const fpath = path.join(DOWNLOAD_DIR, file);
  if (!fs.existsSync(fpath)) return res.sendStatus(404);
  const stat = fs.statSync(fpath);
  serveFileHeaders(res, file, stat);
  return res.sendStatus(200);
});

// -----------------------------
// Webhook: download, serve, attach, cleanup
// -----------------------------
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
  const publicUrl = `${baseHost}/tracks/${filename}`;

  try {
    // 1) Download stream and write to disk
    console.log("🎧 Download start:", soundcloud_url);
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
    if (size < 64 * 1024) console.warn("⚠️ File size unexpectedly small; Airtable may reject fetch.");

    // 2) Ensure our public URL is reachable (HEAD). Retry quickly a few times in case of FS lag.
    let headTries = 0;
    while (headTries < 5) {
      if (await headOk(publicUrl)) break;
      headTries += 1;
      await wait(500 * headTries);
    }
    console.log("🔗 Sending to Airtable:", publicUrl);

    // 3) PATCH Airtable attachment field with public URL
    const airtableUrl = airtableRecordUrl(record_id);
    const patchResp = await fetch(airtableUrl, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${AIRTABLE_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        fields: { "Raw Track Audio File": [{ url: publicUrl, filename }] },
      }),
    });

    const patchJson = await patchResp.json();
    if (!patchResp.ok) {
      console.error("❌ Airtable update failed:", patchJson);
      return res.status(500).json({ error: "Airtable update failed", details: patchJson, sent_url: publicUrl });
    }

    const returnedAtt = patchJson.fields?.["Raw Track Audio File"]?.[0] || null;
    console.log("📦 Airtable returned attachment:", returnedAtt);

    // 4) Respond immediately with debug info
    res.json({
      success: true,
      message: "File processed and pushed to Airtable.",
      sent_url: publicUrl,
      airtable_returned_url: returnedAtt?.url || null,
      airtable_returned: returnedAtt,
    });

    // 5) Poll until Airtable rehosts (airtableusercontent) then cleanup local file
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

// -----------------------------
// Start
// -----------------------------
app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
