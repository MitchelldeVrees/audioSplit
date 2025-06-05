// SplitAudio/index.js
const { tmpdir } = require('os');
const { join }  = require('path');
const crypto    = require('crypto');
const fs        = require('fs').promises;
const { execFile } = require('child_process');
const ffmpegPath   = require('ffmpeg-static');

// Map extensions → MIME
const extToMime = {
  mp3:  'audio/mpeg',
  m4a:  'audio/mp4',
  mp4:  'audio/mp4',
  wav:  'audio/wav',
  ogg:  'audio/ogg',
  oga:  'audio/ogg',
  flac: 'audio/flac',
  webm: 'audio/webm',
  mpeg: 'audio/mpeg',
  mpga: 'audio/mpeg',
};
const supportedExts = new Set(Object.keys(extToMime));
function getExt(filename) {
  const idx = filename.lastIndexOf('.');
  return idx >= 0 ? filename.slice(idx + 1).toLowerCase() : '';
}

// --- splitAudioServer: write buffer to disk, re-encode, and read chunks ---
async function splitAudioServer({ buffer, originalName, mimeType }, segmentSec = 20 * 60) {
  console.log('splitAudioServer(): called');
  if (!ffmpegPath) throw new Error('ffmpeg-static binary not found');

  // 1) Create temp directory
  const tempDir = join(tmpdir(), `split-${crypto.randomUUID()}`);
  console.log(`splitAudioServer(): creating temp directory → ${tempDir}`);
  await fs.mkdir(tempDir, { recursive: true });

  // 2) Determine extension (we’ll re-encode regardless)
  const ext = getExt(originalName) || 'mp3';
  console.log(`splitAudioServer(): originalName="${originalName}", ext=".${ext}", re-encoding to mp3`);

  // 3) Write the incoming buffer to disk (as input.ext)
  const inputPath = join(tempDir, `input.${ext}`);
  console.log(`splitAudioServer(): writing input buffer to ${inputPath} (size=${buffer.length} bytes)`);
  await fs.writeFile(inputPath, buffer);

  // 4) Always re-encode to mp3, then segment
  const outputExt     = 'mp3';
  const outputPattern = join(tempDir, `chunk_%03d.${outputExt}`);
  const ffmpegArgs = [
    '-i', inputPath,
    '-vn',
    '-map', '0:a',
    '-f', 'segment',
    '-segment_time', String(segmentSec),
    '-reset_timestamps', '1',
    '-c:a', 'libmp3lame',
    '-b:a', '128k',
    outputPattern
  ];
  console.log('splitAudioServer(): FFmpeg args →', ffmpegArgs.join(' '));

  // 5) Run FFmpeg
  console.log('splitAudioServer(): spawning FFmpeg process...');
  await new Promise((resolve, reject) => {
    execFile(ffmpegPath, ffmpegArgs, (err, stdout, stderr) => {
      if (err) {
        console.error('splitAudioServer(): FFmpeg error →', err);
        console.error('splitAudioServer(): FFmpeg stderr →', stderr);
        return reject(err);
      }
      console.log('splitAudioServer(): FFmpeg completed successfully');
      if (stdout) console.log('splitAudioServer(): FFmpeg stdout →', stdout);
      if (stderr) console.log('splitAudioServer(): FFmpeg stderr →', stderr);
      resolve();
    });
  });

  // 6) Read all files in tempDir, collect those starting with "chunk_"
  const allFiles = await fs.readdir(tempDir);
  console.log('splitAudioServer(): files in tempDir after FFmpeg →', allFiles);
  const chunks = [];
  for (const filename of allFiles) {
    if (!filename.startsWith('chunk_')) continue;
    const fullPath = join(tempDir, filename);
    const chunkBuffer = await fs.readFile(fullPath);
    // Since we forced mp3 output, its MIME is always audio/mpeg
    const chunkMime = 'audio/mpeg';
    console.log(`splitAudioServer(): found chunk → ${filename} (size=${chunkBuffer.length} bytes, mime=${chunkMime})`);
    chunks.push({ name: filename, mime: chunkMime, buffer: chunkBuffer });
  }

  // 7) Cleanup
  console.log(`splitAudioServer(): removing temp directory → ${tempDir}`);
  await fs.rm(tempDir, { recursive: true, force: true });
  console.log('splitAudioServer(): returning', chunks.length, 'chunks');
  return chunks;
}

// --- Azure Function HTTP trigger (boundary parsing stays the same) ---
module.exports = async function (context, req) {
  context.log('--- SplitAudio invoked ---');

  // 1) Log rawBody length
  const raw = req.rawBody || Buffer.alloc(0);
  context.log(`Request rawBody length: ${raw.length} bytes`);

  // 2) Log first 300 bytes (escaped) for debugging
  const preview = raw
    .slice(0, 300)
    .toString('latin1')
    .replace(/\r/g, '\\r')
    .replace(/\n/g, '\\n');
  context.log(`First 300 bytes of rawBody (escaped):\n${preview}`);

  // 3) Validate Content-Type
  const contentType = req.headers['content-type'] || req.headers['Content-Type'];
  if (!contentType || !contentType.startsWith('multipart/form-data')) {
    context.log.error('Invalid or missing Content-Type:', contentType);
    context.res = { status: 400, body: 'Content-Type must be multipart/form-data' };
    return;
  }
  context.log(`Request Content-Type: ${contentType}`);

  // 4) Extract boundary
  const boundaryMatch = contentType.match(/boundary=(.+)$/);
  if (!boundaryMatch) {
    context.log.error('Cannot find boundary in Content-Type header');
    context.res = { status: 400, body: 'Missing boundary in Content-Type' };
    return;
  }
  const boundary     = boundaryMatch[1];
  context.log(`Extracted boundary → "${boundary}"`);
  const boundaryLine  = `--${boundary}`;
  const closingMarker = `\r\n--${boundary}--`;

  // 5) Convert raw buffer to Latin1 string for header parsing
  const rawStr = raw.toString('latin1');

  // 6) Find where the first part’s headers begin/end
  const firstBoundaryIdx = rawStr.indexOf(boundaryLine);
  if (firstBoundaryIdx < 0) {
    context.log.error('Boundary not found in payload');
    context.res = { status: 400, body: 'Invalid multipart payload' };
    return;
  }
  // Skip past "--boundary\r\n"
  const headerStart = firstBoundaryIdx + boundaryLine.length + 2; 
  const headerEnd   = rawStr.indexOf('\r\n\r\n', headerStart);
  if (headerEnd < 0) {
    context.log.error('Could not find end of headers');
    context.res = { status: 400, body: 'Malformed multipart headers' };
    return;
  }

  // 7) Extract header block and parse name/filename/type
  const headerSection = rawStr.slice(headerStart, headerEnd);
  const nameMatch     = headerSection.match(/name="([^"]+)"/);
  const filenameMatch = headerSection.match(/filename="([^"]+)"/);
  const typeMatch     = headerSection.match(/Content-Type:\s*([^\r\n]+)/i);

  const fieldName = nameMatch     ? nameMatch[1]     : undefined;
  const filename  = filenameMatch ? filenameMatch[1] : undefined;
  const mimeType  = typeMatch     ? typeMatch[1]     : 'application/octet-stream';
  context.log(`Parsed part header: name="${fieldName}", filename="${filename}", type="${mimeType}"`);

  // 8) Ensure the field name is exactly "audioFile"
  if (fieldName !== 'audioFile') {
    context.log.warn('No part named "audioFile" found. Available part name:', fieldName);
    context.res = { status: 400, body: 'Missing form field "audioFile"' };
    return;
  }

  // 9) Find the closing boundary marker to get the full file bytes
  const dataStart  = headerEnd + 4; // skip past "\r\n\r\n"
  const closingIdx = rawStr.indexOf(closingMarker, dataStart);
  if (closingIdx < 0) {
    context.log.error('Closing boundary not found');
    context.res = { status: 400, body: 'Malformed multipart payload (no closing boundary)' };
    return;
  }
  // Extract every byte from dataStart (inclusive) to closingIdx (exclusive)
  const fileBuffer = raw.slice(dataStart, closingIdx);
  context.log(`Extracted fileBuffer (size=${fileBuffer.length} bytes)`);

  // 10) Prepare for splitting
  const fileForSplit = {
    buffer: fileBuffer,
    originalName: filename || 'input',
    mimeType
  };

  // 11) Invoke splitAudioServer
  let chunks;
  try {
    context.log('Invoking splitAudioServer()...');
    chunks = await splitAudioServer(fileForSplit, 20 * 60);
    context.log(`splitAudioServer() returned ${chunks.length} chunk(s)`);
  } catch (splitErr) {
    context.log.error('Error during audio splitting:', splitErr);
    context.res = { status: 500, body: 'Error processing audio' };
    return;
  }

  // 12) Convert each chunk → base64 and return JSON
  context.log('Converting chunks to base64…');
  const payload = await Promise.all(
    chunks.map(async ({ name, mime, buffer }) => {
      context.log(`Encoding chunk "${name}" (size=${buffer.length} bytes, mime=${mime})`);
      const base64data = buffer.toString('base64');
      return { name, mime, data: base64data };
    })
  );

  context.log(`Returning response with ${payload.length} chunk(s)`);
  context.res = {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  };
  context.log('--- SplitAudio completed ---');
};
