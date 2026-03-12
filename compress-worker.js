#!/usr/bin/env node
/**
 * compress-worker.js — GitHub Actions HLS video compression worker
 *
 * Downloads video from SpaceByte → FFmpeg encode to H.264 540p HLS (fmp4) →
 * Parallel-uploads all HLS files to a SpaceByte folder →
 * Calls Oracle VPS callback with HLS manifest data →
 * Reports status to GCP dashboard.
 *
 * Environment variables (set by GitHub Actions):
 *   SPACEBYTE_API_TOKEN        - SpaceByte API auth token
 *   SPACEBYTE_PARENT_FOLDER_ID - SpaceByte parent folder (optional)
 *   VPS_COMPRESS_SECRET        - Shared secret for callback auth
 *   MEDIA_ID                   - Media ID in database (e.g. sb_3100156)
 *   SPACEBYTE_FILE_ID          - SpaceByte file entry ID to download
 *   FILE_NAME                  - Base name for HLS folder
 *   CALLBACK_URL               - Oracle VPS callback endpoint
 *   GCP_STATUS_URL             - GCP dashboard status callback (optional)
 */

const fs = require("fs");
const path = require("path");
const { execSync, spawn } = require("child_process");
const axios = require("axios");
const FormData = require("form-data");

// ─── Config ──────────────────────────────────────────
const SB_TOKEN = process.env.SPACEBYTE_API_TOKEN;
const SB_PARENT = process.env.SPACEBYTE_PARENT_FOLDER_ID || null;
const SECRET = process.env.VPS_COMPRESS_SECRET;
const MEDIA_ID = process.env.MEDIA_ID;
const SB_FILE_ID = process.env.SPACEBYTE_FILE_ID;
const FILE_NAME = (process.env.FILE_NAME || "output").replace(/\.mp4$/i, "");
const CALLBACK_URL = process.env.CALLBACK_URL;
const GCP_STATUS_URL = process.env.GCP_STATUS_URL || "";
const PARALLEL_UPLOADS = 6;
const DOWNLOAD_CHUNKS = 8; // parallel download connections

const SB_API = "https://spacebyte.in/api/v1";
const TEMP_DIR = "/tmp/compress";
const INPUT_FILE = path.join(TEMP_DIR, "input.mp4");
const HLS_DIR = path.join(TEMP_DIR, "hls");

function log(msg) { console.log(`[${new Date().toISOString()}] ${msg}`); }

// Report status to GCP dashboard (fire-and-forget)
function reportStatus(status, step, progress) {
  if (!GCP_STATUS_URL) return;
  axios.post(GCP_STATUS_URL, {
    mediaId: MEDIA_ID,
    status,
    step,
    progress: progress || 0,
  }, { timeout: 5000 }).catch(() => {});
}

// ─── Step 1: Download source video from SpaceByte ────
async function getSignedUrl() {
  try {
    const resp = await axios.get(`${SB_API}/file-entries/${SB_FILE_ID}`, {
      headers: { Authorization: `Bearer ${SB_TOKEN}`, Accept: "application/json" },
      maxRedirects: 0,
      validateStatus: s => s >= 200 && s < 400,
      timeout: 15000
    }).catch(err => {
      if (err.response && err.response.status === 302) return err.response;
      throw err;
    });

    if (resp.status === 302 || resp.status === 301) {
      return resp.headers.location;
    }
    const r2 = await axios.get(`${SB_API}/file-entries/${SB_FILE_ID}`, {
      headers: { Authorization: `Bearer ${SB_TOKEN}` },
      maxRedirects: 5,
      responseType: "stream",
      timeout: 15000
    });
    const url = r2.request?.res?.responseUrl;
    r2.data?.destroy?.();
    return url;
  } catch (err) {
    throw new Error(`Failed to get signed URL: ${err.message}`);
  }
}

async function downloadChunk(url, start, end, partFile) {
  const resp = await axios.get(url, {
    responseType: "stream",
    timeout: 600000,
    headers: { Range: `bytes=${start}-${end}` }
  });
  const writer = fs.createWriteStream(partFile);
  resp.data.pipe(writer);
  await new Promise((res, rej) => {
    writer.on("finish", res);
    writer.on("error", rej);
    resp.data.on("error", rej);
  });
}

async function download() {
  log(`Downloading SpaceByte file ${SB_FILE_ID}...`);
  reportStatus("running", "Downloading from SpaceByte...", 0);

  const signedUrl = await getSignedUrl();
  if (!signedUrl) throw new Error("No signed URL resolved");

  // Get file size with HEAD request
  const head = await axios.head(signedUrl, { timeout: 15000 });
  const totalSize = parseInt(head.headers["content-length"] || "0", 10);
  const acceptRanges = (head.headers["accept-ranges"] || "").toLowerCase();

  // Fall back to single-stream if server doesn't support ranges or file is small
  if (!totalSize || acceptRanges === "none" || totalSize < 50 * 1024 * 1024) {
    log("Using single-stream download");
    const resp = await axios.get(signedUrl, { responseType: "stream", timeout: 600000, maxRedirects: 5 });
    const writer = fs.createWriteStream(INPUT_FILE);
    let downloaded = 0;
    resp.data.on("data", chunk => {
      downloaded += chunk.length;
      if (totalSize > 0 && downloaded % (5 * 1024 * 1024) < chunk.length) {
        const pct = Math.round((downloaded / totalSize) * 100);
        reportStatus("running", `Downloading: ${pct}%`, pct);
      }
    });
    resp.data.pipe(writer);
    await new Promise((res, rej) => { writer.on("finish", res); writer.on("error", rej); resp.data.on("error", rej); });
  } else {
    // Parallel chunk download
    const chunks = DOWNLOAD_CHUNKS;
    const chunkSize = Math.ceil(totalSize / chunks);
    log(`Parallel download: ${chunks} connections, ${(totalSize / 1024 / 1024).toFixed(0)} MB total`);

    const partFiles = [];
    const promises = [];
    for (let i = 0; i < chunks; i++) {
      const start = i * chunkSize;
      const end = Math.min(start + chunkSize - 1, totalSize - 1);
      const partFile = path.join(TEMP_DIR, `part_${i}`);
      partFiles.push(partFile);
      promises.push(downloadChunk(signedUrl, start, end, partFile).then(() => {
        const done = partFiles.filter(f => fs.existsSync(f)).length;
        const pct = Math.round((done / chunks) * 100);
        reportStatus("running", `Downloading: ${pct}%`, pct);
      }));
    }
    await Promise.all(promises);

    // Merge chunks into final file
    const writer = fs.createWriteStream(INPUT_FILE);
    for (const pf of partFiles) {
      await new Promise((res, rej) => {
        const reader = fs.createReadStream(pf);
        reader.pipe(writer, { end: false });
        reader.on("end", () => { fs.unlinkSync(pf); res(); });
        reader.on("error", rej);
      });
    }
    writer.end();
    await new Promise(res => writer.on("finish", res));
  }

  const size = fs.statSync(INPUT_FILE).size;
  log(`Downloaded: ${(size / 1024 / 1024).toFixed(1)} MB`);
  reportStatus("running", `Downloaded: ${(size / 1024 / 1024).toFixed(0)} MB`, 100);
  return size;
}

// ─── Step 2+3: Encode HLS + stream-upload segments as they're created ──
async function createSpaceBytFolder() {
  const folderName = `hls_${FILE_NAME}_${Date.now()}`.slice(0, 200);
  log(`Creating SpaceByte folder: ${folderName}`);

  const folderResp = await axios.post(`${SB_API}/folders`, {
    name: folderName,
    ...(SB_PARENT ? { parentId: String(SB_PARENT) } : {})
  }, {
    headers: { Authorization: `Bearer ${SB_TOKEN}`, "Content-Type": "application/json" },
    timeout: 15000
  });

  const folderId = folderResp.data?.folder?.id || folderResp.data?.id || folderResp.data?.data?.id;
  if (!folderId) throw new Error(`Folder creation failed: ${JSON.stringify(folderResp.data).slice(0, 300)}`);
  log(`Folder created: ID ${folderId}`);
  return String(folderId);
}

async function uploadOneFile(fileName, folderId) {
  const filePath = path.join(HLS_DIR, fileName);
  const fileSize = fs.statSync(filePath).size;
  const isManifest = fileName.endsWith(".m3u8");
  const contentType = isManifest ? "application/vnd.apple.mpegurl"
    : fileName.endsWith(".mp4") ? "video/mp4"
    : "video/iso.segment";

  const form = new FormData();
  form.append("file", fs.createReadStream(filePath), {
    filename: fileName,
    contentType,
    knownLength: fileSize
  });
  form.append("parentId", String(folderId));

  const resp = await axios.post(`${SB_API}/uploads`, form, {
    headers: { ...form.getHeaders(), Authorization: `Bearer ${SB_TOKEN}` },
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    timeout: 300000
  });

  const id = resp.data?.fileEntry?.id || resp.data?.id || resp.data?.data?.id;
  if (!id) throw new Error(`Upload ${fileName} failed: no ID returned`);
  return String(id);
}

async function encodeAndStreamUpload() {
  log("Encoding HLS + streaming upload of segments...");
  reportStatus("running", "Encoding HLS + uploading segments...", 0);
  fs.mkdirSync(HLS_DIR, { recursive: true });

  // Create SpaceByte folder first
  const folderId = await createSpaceBytFolder();
  const fileMap = {};

  // Probe video duration
  let duration = 0;
  try {
    const probe = execSync(`ffprobe -v error -show_streams -show_format -of json "${INPUT_FILE}"`, { timeout: 30000 }).toString();
    const info = JSON.parse(probe);
    const video = info.streams?.find(s => s.codec_type === "video");
    duration = parseFloat(info.format?.duration || "0");
    if (video) log(`Input: ${video.width}x${video.height} ${video.codec_name} duration=${Math.round(duration)}s`);
  } catch (_) {}

  const manifestPath = path.join(HLS_DIR, "index.m3u8");
  const args = [
    "-y", "-i", INPUT_FILE,
    "-c:v", "libx264",
    "-profile:v", "high",
    "-level:v", "4.1",
    "-crf", "26",
    "-preset", "medium",
    "-vf", "scale=-2:'min(540,ih)'",
    "-pix_fmt", "yuv420p",
    "-g", "48",
    "-keyint_min", "48",
    "-sc_threshold", "0",
    "-c:a", "aac", "-b:a", "128k", "-ac", "2",
    "-f", "hls",
    "-hls_time", "4",
    "-hls_playlist_type", "vod",
    "-hls_flags", "independent_segments",
    "-hls_segment_type", "fmp4",
    "-hls_fmp4_init_filename", "init.mp4",
    "-hls_segment_filename", path.join(HLS_DIR, "seg_%05d.m4s"),
    "-sn", "-dn",
    manifestPath
  ];

  // Track segments: uploaded set, upload queue, and active upload promises
  const uploadedSet = new Set();
  const uploadQueue = [];
  const activeUploads = [];
  let encodingDone = false;
  let totalSegments = 0;
  let uploadedCount = 0;

  // Upload worker: processes uploadQueue with PARALLEL_UPLOADS concurrency
  async function uploadWorker() {
    while (true) {
      // Fill active slots
      while (activeUploads.length < PARALLEL_UPLOADS && uploadQueue.length > 0) {
        const fileName = uploadQueue.shift();
        const p = uploadOneFile(fileName, folderId)
          .then(id => {
            fileMap[fileName] = id;
            uploadedCount++;
            if (uploadedCount % 20 === 0) log(`  Streamed: ${uploadedCount} segments uploaded`);
          })
          .catch(err => {
            log(`  Retry upload: ${fileName} (${err.message.slice(0, 80)})`);
            return uploadOneFile(fileName, folderId).then(id => {
              fileMap[fileName] = id;
              uploadedCount++;
            });
          });
        activeUploads.push(p);
      }

      if (activeUploads.length > 0) {
        await Promise.race(activeUploads);
        // Remove settled promises
        for (let i = activeUploads.length - 1; i >= 0; i--) {
          const done = await Promise.race([activeUploads[i].then(() => true), Promise.resolve(false)]);
          if (done) activeUploads.splice(i, 1);
        }
      } else if (encodingDone && uploadQueue.length === 0) {
        break;
      } else {
        // No work yet, wait a bit
        await new Promise(r => setTimeout(r, 500));
      }
    }
  }

  // File watcher: detect new segments and queue them for upload
  let knownFiles = new Set();
  const watchInterval = setInterval(() => {
    if (!fs.existsSync(HLS_DIR)) return;
    const files = fs.readdirSync(HLS_DIR);
    for (const f of files) {
      if (knownFiles.has(f) || uploadedSet.has(f)) continue;
      // Skip the manifest — upload it last after encoding completes
      if (f === "index.m3u8") continue;
      // For segments: only upload if the next segment exists (meaning this one is complete)
      // For init.mp4: upload immediately
      if (f === "init.mp4") {
        knownFiles.add(f);
        uploadedSet.add(f);
        uploadQueue.push(f);
        log("  init.mp4 ready → queued for upload");
      } else if (f.endsWith(".m4s")) {
        // Check if a newer segment exists (meaning this one is finalized)
        const match = f.match(/seg_(\d+)\.m4s/);
        if (!match) continue;
        const num = parseInt(match[1], 10);
        const nextSeg = `seg_${String(num + 1).padStart(5, "0")}.m4s`;
        // Upload if next segment exists OR encoding is done (last segment)
        if (files.includes(nextSeg) || encodingDone) {
          knownFiles.add(f);
          uploadedSet.add(f);
          uploadQueue.push(f);
          totalSegments++;
        }
      }
    }
  }, 1000);

  // Start FFmpeg encode
  const encodePromise = new Promise((resolve, reject) => {
    const child = spawn("ffmpeg", args, { stdio: ["ignore", "ignore", "pipe"] });
    let stderr = "";
    child.stderr.on("data", chunk => {
      stderr += chunk.toString();
      if (stderr.length > 8000) stderr = stderr.slice(-8000);
      const m = stderr.match(/time=(\d+):(\d+):(\d+(?:\.\d+)?)/g);
      if (m && m.length) {
        const timeStr = m[m.length - 1];
        if (duration > 0) {
          const parts = timeStr.replace("time=", "").split(":");
          const secs = parseInt(parts[0]) * 3600 + parseInt(parts[1]) * 60 + parseFloat(parts[2]);
          const pct = Math.min(99, Math.round((secs / duration) * 100));
          const uploadInfo = uploadedCount > 0 ? ` | ${uploadedCount} segs uploaded` : "";
          reportStatus("running", `Encoding HLS: ${pct}%${uploadInfo}`, pct);
        }
      }
    });
    child.on("close", code => {
      console.log("");
      encodingDone = true;
      if (code === 0) {
        try { fs.unlinkSync(INPUT_FILE); } catch (_) {}
        resolve();
      } else {
        reject(new Error(`FFmpeg failed (code ${code}): ${stderr.slice(-300)}`));
      }
    });
    child.on("error", reject);
  });

  // Start upload worker in parallel with encoding
  const uploadPromise = uploadWorker();

  // Wait for encoding to finish first
  await encodePromise;

  // Final watcher pass to catch the last segment(s)
  if (fs.existsSync(HLS_DIR)) {
    const files = fs.readdirSync(HLS_DIR);
    for (const f of files) {
      if (uploadedSet.has(f) || f === "index.m3u8") continue;
      if (f === "init.mp4" || f.endsWith(".m4s")) {
        uploadedSet.add(f);
        uploadQueue.push(f);
        totalSegments++;
      }
    }
  }

  // Wait for all uploads to finish
  clearInterval(watchInterval);
  await uploadPromise;

  // Drain any remaining active uploads
  if (activeUploads.length > 0) {
    await Promise.all(activeUploads);
  }

  // Now upload the final manifest
  log("Uploading final manifest (index.m3u8)...");
  reportStatus("running", "Uploading manifest...", 99);
  const manifestId = await uploadOneFile("index.m3u8", folderId);
  fileMap["index.m3u8"] = manifestId;
  uploadedCount++;

  const allFiles = fs.readdirSync(HLS_DIR);
  const totalSize = allFiles.reduce((sum, f) => sum + fs.statSync(path.join(HLS_DIR, f)).size, 0);
  log(`HLS encode + upload done: ${allFiles.length} files, ${(totalSize / 1024 / 1024).toFixed(1)} MB, ${uploadedCount} uploaded`);

  return {
    folderId,
    manifestFileId: manifestId,
    fileMap,
    totalSize,
    segmentCount: allFiles.length,
    files: allFiles
  };
}

// ─── Step 4: Delete old SpaceByte file ───────────────
async function deleteOldFile() {
  if (!SB_FILE_ID) return;
  log(`Deleting original SpaceByte file ${SB_FILE_ID}...`);
  reportStatus("running", "Deleting original file...", 0);
  try {
    await axios.delete(`${SB_API}/file-entries/${Number(SB_FILE_ID)}`, {
      headers: { Authorization: `Bearer ${SB_TOKEN}` },
      timeout: 15000
    });
    log(`Deleted original file ${SB_FILE_ID}`);
  } catch (err) {
    log(`Warning: Failed to delete original file ${SB_FILE_ID}: ${err.message}`);
  }
}

// ─── Step 5: Callback to Oracle VPS ──────────────────
async function callback(hlsData) {
  log(`Calling back to VPS: ${CALLBACK_URL}`);
  reportStatus("running", "Updating VPS database...", 0);

  const resp = await axios.post(CALLBACK_URL, {
    mediaId: MEDIA_ID,
    oldSpacebyteFileId: SB_FILE_ID || undefined,
    hls: {
      folderId: hlsData.folderId,
      manifestFileId: hlsData.manifestFileId,
      segmentCount: hlsData.segmentCount,
      totalSize: hlsData.totalSize
    },
    secret: SECRET
  }, {
    timeout: 30000,
    headers: { "Content-Type": "application/json" }
  });

  if (resp.data?.ok) {
    log("VPS confirmed HLS update");
  } else {
    log(`VPS response: ${JSON.stringify(resp.data).slice(0, 300)}`);
  }
}

// ─── Main ────────────────────────────────────────────
async function main() {
  if (!SB_TOKEN || !MEDIA_ID || !CALLBACK_URL || !SB_FILE_ID) {
    console.error("Missing required env vars: SPACEBYTE_API_TOKEN, MEDIA_ID, CALLBACK_URL, SPACEBYTE_FILE_ID");
    process.exit(1);
  }

  fs.mkdirSync(TEMP_DIR, { recursive: true });

  log(`=== HLS Compress Job: ${MEDIA_ID} (SpaceByte ${SB_FILE_ID}) ===`);
  reportStatus("running", "Starting...", 0);

  const originalSize = await download();
  const hlsData = await encodeAndStreamUpload();

  const reduction = originalSize > 0 ? ((1 - hlsData.totalSize / originalSize) * 100).toFixed(1) : 0;
  log(`Size reduction: ${reduction}% (${(originalSize / 1024 / 1024).toFixed(1)} MB → ${(hlsData.totalSize / 1024 / 1024).toFixed(1)} MB)`);

  await deleteOldFile();
  await callback(hlsData);

  try { fs.rmSync(HLS_DIR, { recursive: true, force: true }); } catch (_) {}

  reportStatus("completed", "Done!", 100);
  log("=== Done! ===");
}

main().catch(err => {
  console.error(`\nFatal: ${err.message}`);
  reportStatus("failed", err.message.slice(0, 200), 0);
  if (CALLBACK_URL && SECRET) {
    axios.post(CALLBACK_URL, {
      mediaId: MEDIA_ID,
      error: err.message,
      secret: SECRET
    }, { timeout: 10000 }).catch(() => {});
  }
  process.exit(1);
});
