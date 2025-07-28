const express = require("express");
const fs = require("fs");
const path = require("path");
const bodyParser = require("body-parser");
const { exec } = require("child_process");
// ✅ Correct (with .default for CommonJS compatibility)
const fetch = require("node-fetch").default;

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

app.get("/tracks/:filename", (req, res) => {
  const f = path.join(DOWNLOAD_DIR, req.params.filename);
  if (fs.existsSync(f)) {
    res.setHeader("Content-Type", "audio/mpeg");
    res.sendFile(f);
  } else {
    res.status(404).send("Not found");
  }
});

app.post("/webhook", async (req, res) => {
  const { record_id, soundcloud_url } = req.body;
  if (!record_id || !soundcloud_url) {
    console.error("❌ Missing required fields in request body");
    return res.status(400).json({ error: "Missing record_id or soundcloud_url" });
  }

  const filename = `${record_id}.mp3`;
  const filepath = path.join(DOWNLOAD_DIR, filename);
  const publicUrl = `${HOST}/tracks/${filename}`;

  console.log(`📥 Starting download for: ${soundcloud_url}`);

  exec(`yt-dlp -x --audio-format mp3 -o "${filepath}" "${soundcloud_url}"`, async (err) => {
    if (err) {
      console.error("❌ Download failed:", err);
      return res.status(500).json({ error: "Download failed" });
    }

    console.log(`✅ Downloaded to ${filepath}`);

    // PATCH Airtable record using REST API
    try {
      const airtableUrl = `https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(TABLE)}/${record_id}`;
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

      if (!patchResp.ok) {
        const errorText = await patchResp.text();
        console.error("❌ Airtable update failed:", errorText);
        return res.status(500).json({ error: "Airtable update failed", details: errorText });
      }

      console.log(`📤 Uploaded public URL to Airtable for record ${record_id}`);
    } catch (err) {
      console.error("❌ Request error:", err);
      return res.status(500).json({ error: "Request failed", details: err.message });
    }

    // Cleanup after Airtable rehosts it
    setTimeout(async () => {
      try {
        const recordUrl = `https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(TABLE)}/${record_id}`;
        const response = await fetch(recordUrl, {
          headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
        });

        const record = await response.json();
        const att = record.fields?.["Raw Track Audio File"];
        const isAirtableHosted = att?.[0]?.url?.includes("airtableusercontent");

        if (isAirtableHosted && fs.existsSync(filepath)) {
          fs.unlink(filepath, () => {});
          console.log(`🧹 Cleaned up ${filename}`);
        } else {
          console.log(`⏳ Cleanup skipped: Airtable hasn't rehosted or file missing`);
        }
      } catch (e) {
        console.error("⚠️ Polling error during cleanup:", e);
      }
    }, 20000);

    return res.json({ success: true, message: "File processed and pushed to Airtable." });
  });
});

app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
