const express = require("express");
const fs = require("fs");
const path = require("path");
const bodyParser = require("body-parser");
const fetch = require("node-fetch").default;
const scdl = require("soundcloud-downloader").default;
require("dotenv").config();

const app = express();
app.use(bodyParser.json());

const PORT = process.env.PORT || 3000;
const DOWNLOAD_DIR = path.join(__dirname, "tracks");

if (!fs.existsSync(DOWNLOAD_DIR)) fs.mkdirSync(DOWNLOAD_DIR);

const BASE_ID = process.env.AIRTABLE_BASE_ID;
const TABLE = process.env.AIRTABLE_TABLE_NAME;
const HOST = process.env.HOST_URL;
const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY;
const CLIENT_ID = process.env.SOUNDCLOUD_CLIENT_ID;
const SAVE_CLIENT_ID = (process.env.SAVE_CLIENT_ID || "false").toLowerCase() === "true";

// Simple health check
app.get("/health", (req, res) => res.json({ ok: true }));

// Serve MP3 with explicit headers Airtable's fetcher likes
app.get("/tracks/:filename", (req, res) => {
  const f = path.join(DOWNLOAD_DIR, req.params.filename);
  if (fs.existsSync(f)) {
    const stat = fs.statSync(f);
    res.setHeader("Content-Type", "audio/mpeg");
    res.setHeader("Content-Length", stat.size);
    res.setHeader("Accept-Ranges", "bytes");
    res.setHeader("Cache-Control", "public, max-age=3600");
    res.setHeader("Content-Disposition", `inline; filename="${req.params.filename}"`);
    res.sendFile(f);
  } else {
    res.status(404).send("Not found");
  }
});

app.post("/webhook", async (req, res) => {
  const { record_id, soundcloud_url } = req.body;
  if (!record_id || !soundcloud_url) {
    return res.status(400).json({ error: "Missing record_id or soundcloud_url" });
  }

  const filename = `${record_id}.mp3`;
  const filepath = path.join(DOWNLOAD_DIR, filename);
  const publicUrl = `${HOST}/tracks/${filename}`;

  try {
    // Download stream and write to disk
    const stream = await scdl.download(soundcloud_url, CLIENT_ID);
    const writeStream = fs.createWriteStream(filepath);
    stream.pipe(writeStream);

    writeStream.on("finish", async () => {
      console.log(`✅ Downloaded to ${filepath}`);

      try {
        const airtableUrl = `https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(TABLE)}/${record_id}`;
        console.log("🔗 Sending to Airtable:", publicUrl);

        const patchResp = await fetch(airtableUrl, {
          method: "PATCH",
          headers: {
            Authorization: `Bearer ${AIRTABLE_API_KEY}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            fields: {
              "Raw Track Audio File": [{ url: publicUrl, filename }]
            }
          })
        });

        const result = await patchResp.json();
        if (!patchResp.ok) {
          console.error("❌ Airtable update failed:", result);
          return res.status(500).json({ error: "Airtable update failed", details: result });
        }

        const returnedAtt = result.fields?.["Raw Track Audio File"]?.[0] || null;
        console.log("📦 Airtable returned attachment:", returnedAtt);

        // Respond immediately with the URLs for debugging
        res.json({
          success: true,
          message: "File processed and pushed to Airtable.",
          sent_url: publicUrl,
          airtable_returned_url: returnedAtt?.url || null,
          airtable_returned: returnedAtt
        });

        // Poll for rehosting and cleanup (up to 90s)
        const started = Date.now();
        const poll = async () => {
          try {
            const pollResp = await fetch(airtableUrl, {
              headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
            });
            const record = await pollResp.json();
            const att = record.fields?.["Raw Track Audio File"];
            const currentUrl = att?.[0]?.url || null;
            const isAirtableHosted = currentUrl?.includes("airtableusercontent");
            console.log("🔎 Current attachment URL:", currentUrl);

            if (isAirtableHosted) {
              if (fs.existsSync(filepath)) fs.unlink(filepath, () => {});
              console.log(`🧹 Cleaned up ${filename}`);
              return; // stop polling
            }

            if (Date.now() - started < 90000) {
              setTimeout(poll, 10000);
            } else {
              console.log("⏳ Gave up waiting for Airtable to rehost after 90s");
            }
          } catch (e) {
            console.error("⚠️ Polling error during cleanup:", e);
          }
        };
        setTimeout(poll, 10000);
      } catch (uploadErr) {
        console.error("❌ Upload to Airtable failed:", uploadErr);
        return; // response already sent if we reached here earlier
      }
    });

    writeStream.on("error", (err) => {
      console.error("❌ Failed to write MP3 file:", err);
      return res.status(500).json({ error: "Failed to save MP3" });
    });
  } catch (err) {
    console.error("❌ SoundCloud download failed:", err);
    return res.status(500).json({ error: "SoundCloud download failed", details: String(err?.message || err) });
  }
});

app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
