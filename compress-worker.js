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
const PARALLEL_UPLOADS = 15;

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

// ─── Step 1: Download source video from SpaceByte (aria2c 16 connections) ────
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

async function download() {
  log(`Downloading SpaceByte file ${SB_FILE_ID}...`);
  reportStatus("running", "Downloading from SpaceByte...", 0);

  const signedUrl = await getSignedUrl();
  if (!signedUrl) throw new Error("No signed URL resolved");

  const dir = path.dirname(INPUT_FILE);
  const out = path.basename(INPUT_FILE);

  return new Promise((resolve, reject) => {
    const args = [
      "-x16", "-s16",
      "--max-connection-per-server=16",
      "-k1M",
      "--file-allocation=none",
      "--auto-file-renaming=false",
      "--allow-overwrite=true",
      "--summary-interval=3",
      "--console-log-level=notice",
      "--download-result=hide",
      "-d", dir,
      "-o", out,
      signedUrl,
    ];

    const proc = spawn("aria2c", args, { stdio: ["pipe", "pipe", "pipe"] });
    let stderr = "";
    const startTime = Date.now();

    proc.stdout.on("data", (data) => {
      const line = data.toString();
      const dlMatch = line.match(/(\d+(?:\.\d+)?[KMGT]?i?B)\/(\d+(?:\.\d+)?[KMGT]?i?B)\((\d+)%\)/);
      const speedMatch = line.match(/DL:(\d+(?:\.\d+)?[KMGT]?i?B)/);
      if (dlMatch) {
        const pct = parseInt(dlMatch[3], 10);
        const speedStr = speedMatch ? ` @ ${speedMatch[1]}/s` : "";
        reportStatus("running", `Downloading: ${dlMatch[3]}%${speedStr}`, pct);
      }
    });

    proc.stderr.on("data", (data) => { stderr += data.toString(); });

    proc.on("close", (code) => {
      if (code === 0 && fs.existsSync(INPUT_FILE)) {
        const elapsed = (Date.now() - startTime) / 1000;
        const size = fs.statSync(INPUT_FILE).size;
        const speed = size / elapsed;
        log(`Downloaded: ${(size / 1024 / 1024).toFixed(1)} MB in ${Math.round(elapsed)}s (${(speed / 1024 / 1024).toFixed(1)} MB/s)`);
        reportStatus("running", `Downloaded: ${(size / 1024 / 1024).toFixed(0)} MB`, 100);
        resolve(size);
      } else {
        reject(new Error(`aria2c exit ${code}: ${stderr.slice(-300)}`));
      }
    });
    proc.on("error", reject);
  });
}

// ─── Step 2: Create SpaceByte folder ─────────────────────────
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

// ─── Upload helper ───────────────────────────────────────────
async function uploadOneFile(fileName, folderId) {
  const filePath = path.join(HLS_DIR, fileName);
  const fileSize = fs.statSync(filePath).size;
  const isManifest = fileName.endsWith(".m3u8");
  const contentType = isManifest ? "application/vnd.apple.mpegurl"
    : fileName.endsWith(".mp4") ? "video/mp4"
    : "video/iso.segment";

  const form = new FormData();
  const stream = fs.createReadStream(filePath);
  form.append("file", stream, {
    filename: fileName,
    contentType,
    knownLength: fileSize
  });
  form.append("parentId", String(folderId));

  try {
    const resp = await axios.post(`${SB_API}/uploads`, form, {
      headers: { ...form.getHeaders(), Authorization: `Bearer ${SB_TOKEN}` },
      maxContentLength: Infinity,
      maxBodyLength: Infinity,
      timeout: 60000
    });

    const id = resp.data?.fileEntry?.id || resp.data?.id || resp.data?.data?.id;
    if (!id) throw new Error(`Upload ${fileName} failed: no ID returned`);
    return String(id);
  } finally {
    stream.destroy();
  }
}

// ─── Step 3: Encode HLS ──────────────────────────────────────
async function encodeHLS() {
  log("Encoding HLS...");
  reportStatus("running", "Encoding HLS...", 0);
  fs.mkdirSync(HLS_DIR, { recursive: true });

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

  await new Promise((resolve, reject) => {
    const child = spawn("ffmpeg", args, { stdio: ["ignore", "ignore", "pipe"] });
    let stderr = "";
    child.stderr.on("data", chunk => {
      stderr += chunk.toString();
      if (stderr.length > 8000) stderr = stderr.slice(-8000);
      const m = stderr.match(/time=(\d+):(\d+):(\d+(?:\.\d+)?)/g);
      if (m && m.length && duration > 0) {
        const timeStr = m[m.length - 1];
        const parts = timeStr.replace("time=", "").split(":");
        const secs = parseInt(parts[0]) * 3600 + parseInt(parts[1]) * 60 + parseFloat(parts[2]);
        const pct = Math.min(99, Math.round((secs / duration) * 100));
        reportStatus("running", `Encoding HLS: ${pct}%`, pct);
      }
    });
    child.on("close", code => {
      console.log("");
      if (code === 0) {
        try { fs.unlinkSync(INPUT_FILE); } catch (_) {}
        resolve();
      } else {
        reject(new Error(`FFmpeg failed (code ${code}): ${stderr.slice(-300)}`));
      }
    });
    child.on("error", reject);
  });

  const files = fs.readdirSync(HLS_DIR);
  const totalSize = files.reduce((sum, f) => sum + fs.statSync(path.join(HLS_DIR, f)).size, 0);
  log(`Encode done: ${files.length} files, ${(totalSize / 1024 / 1024).toFixed(1)} MB`);
  return { files, totalSize, duration };
}

// ─── Step 3: Upload HLS files in controlled batches ──────────
async function uploadHLSFiles(hlsFiles) {
  const folderId = await createSpaceBytFolder();
  const fileMap = {};
  let uploaded = 0;

  log(`Uploading ${hlsFiles.length} HLS files (${PARALLEL_UPLOADS} parallel)...`);
  reportStatus("running", `Uploading HLS: 0/${hlsFiles.length}`, 0);

  // Upload manifest last
  const manifest = hlsFiles.find(f => f === "index.m3u8");
  const segments = hlsFiles.filter(f => f !== "index.m3u8");

  // Sliding-window concurrency pool — keeps all slots busy
  const queue = [...segments];
  const active = new Map(); // promise → fileName
  const errors = [];

  function launchOne() {
    if (!queue.length) return;
    const fileName = queue.shift();
    const p = (async () => {
      try {
        const id = await uploadOneFile(fileName, folderId);
        fileMap[fileName] = id;
      } catch (err) {
        log(`  Retry: ${fileName} (${err.message.slice(0, 80)})`);
        try {
          const id = await uploadOneFile(fileName, folderId);
          fileMap[fileName] = id;
        } catch (err2) {
          errors.push(`${fileName}: ${err2.message}`);
        }
      }
      uploaded++;
      const pct = Math.round((uploaded / hlsFiles.length) * 100);
      if (uploaded % 30 === 0) {
        log(`  Uploaded: ${uploaded}/${hlsFiles.length} (${pct}%)`);
      }
      reportStatus("running", `Uploading HLS: ${uploaded}/${hlsFiles.length} (${pct}%)`, pct);
    })();
    active.set(p, fileName);
    p.finally(() => active.delete(p));
    return p;
  }

  // Fill initial pool
  while (active.size < PARALLEL_UPLOADS && queue.length) launchOne();

  // As each completes, launch the next
  while (active.size > 0) {
    await Promise.race([...active.keys()]);
    while (active.size < PARALLEL_UPLOADS && queue.length) launchOne();
  }

  if (errors.length) throw new Error(`Upload failed for ${errors.length} file(s): ${errors[0]}`);
  log(`  Uploaded: ${uploaded}/${hlsFiles.length} (segments done)`);

  // Upload manifest last
  if (manifest) {
    log("Uploading final manifest (index.m3u8)...");
    const manifestId = await uploadOneFile(manifest, folderId);
    fileMap[manifest] = manifestId;
    uploaded++;
  }

  const manifestFileId = fileMap["index.m3u8"];
  if (!manifestFileId) throw new Error("Manifest file (index.m3u8) was not uploaded");

  const totalSize = hlsFiles.reduce((sum, f) => sum + fs.statSync(path.join(HLS_DIR, f)).size, 0);
  log(`Upload done: ${uploaded} files, manifest ID: ${manifestFileId}`);

  return { folderId, manifestFileId, fileMap, totalSize, segmentCount: hlsFiles.length, files: hlsFiles };
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
      totalSize: hlsData.totalSize,
      fileMap: hlsData.fileMap
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
  const { files: hlsFiles } = await encodeHLS();
  const hlsData = await uploadHLSFiles(hlsFiles);

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
