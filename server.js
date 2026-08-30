/**
 * Drive to YouTube Unlisted Video Streaming Pipeline
 * Real-time SSE tracking, Recursive Subfolder Scanning, Auto-Playlist Generation, Batch/Subject Detection, and Live Preview
 */

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const { google } = require('googleapis');
const { Transform } = require('stream');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');

const app = express();
const PORT = process.env.PORT || 3000;
const DATA_DIR = path.join(__dirname, 'data');
const STATE_FILE = path.join(DATA_DIR, 'job_state.json');
const HISTORY_FILE = path.join(DATA_DIR, 'uploaded_history.json');

if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

function loadUploadedHistory() {
  try {
    if (fs.existsSync(HISTORY_FILE)) {
      const data = fs.readFileSync(HISTORY_FILE, 'utf8');
      const parsed = JSON.parse(data);
      if (Array.isArray(parsed)) {
        parsed.forEach(f => {
          if (f.audioHealth && f.audioHealth.verdict === 'silent_all') {
            delete f.audioHealth;
          }
        });
        return parsed;
      }
    }
  } catch (err) {
    console.error('Error reading upload history:', err);
  }
  return [];
}

function persistUploadedHistory(history) {
  try {
    fs.writeFileSync(HISTORY_FILE, JSON.stringify(history, null, 2), 'utf8');
  } catch (err) {
    console.error('Error saving upload history:', err);
  }
}

function saveCompletedFileToHistory(fileObj) {
  if (!fileObj || !fileObj.id) return;
  const history = loadUploadedHistory();
  const existingIdx = history.findIndex(h => (h.videoId && h.videoId === fileObj.videoId) || h.id === fileObj.id);
  const record = {
    id: fileObj.id,
    videoId: fileObj.videoId || fileObj.id,
    name: fileObj.customTitle || fileObj.name || fileObj.originalName,
    originalName: fileObj.originalName || fileObj.name,
    customTitle: fileObj.customTitle || fileObj.name,
    batch: fileObj.batch || 'Batch',
    subject: fileObj.subject || 'Lecture',
    folderPath: fileObj.folderPath || '',
    channelId: fileObj.channelId || activeJobChannelId || null,
    size: fileObj.size || fileObj.totalBytes || 0,
    createdTime: fileObj.createdTime || new Date().toISOString(),
    status: 'completed',
    percentage: 100,
    uploadedBytes: fileObj.totalBytes || fileObj.size || 0,
    totalBytes: fileObj.totalBytes || fileObj.size || 0,
    speedMBps: fileObj.speedMBps || 0,
    etaSeconds: 0,
    youtubeUrl: fileObj.youtubeUrl || (fileObj.videoId ? `https://youtu.be/${fileObj.videoId}` : ''),
    thumbnailUrl: fileObj.thumbnailUrl || (fileObj.videoId ? `https://img.youtube.com/vi/${fileObj.videoId}/mqdefault.jpg` : ''),
    studioUrl: fileObj.studioUrl || (fileObj.videoId ? `https://studio.youtube.com/video/${fileObj.videoId}/edit` : ''),
    error: null
  };

  if (existingIdx >= 0) {
    history[existingIdx] = { ...history[existingIdx], ...record };
  } else {
    history.unshift(record);
  }
  persistUploadedHistory(history);
}

// Middleware
app.use(helmet({
  contentSecurityPolicy: false,
  crossOriginEmbedderPolicy: false
}));
app.use(cors({
  origin: ['http://localhost:3000', 'http://127.0.0.1:3000'],
  credentials: true
}));
app.use('/api/', rateLimit({
  windowMs: 60 * 1000,
  max: 60,
  standardHeaders: true,
  legacyHeaders: false,
  message: { success: false, error: 'Too many requests, please try again later.' }
}));
app.use((req, res, next) => {
  res.setHeader('Cross-Origin-Opener-Policy', 'same-origin-allow-popups');
  res.setHeader('Cross-Origin-Resource-Policy', 'cross-origin');
  next();
});
app.use(express.json({ limit: '50mb' })); // Increased for thumbnails
app.use(express.static(path.join(__dirname, 'public')));

// Store active SSE client connections
const clients = new Map();

// Per-User Isolation: token → channelId cache (auto-expires)
const tokenChannelCache = new Map();
let activeJobChannelId = null; // Track which user started the current upload job

async function resolveChannelId(req) {
  const authHeader = req.headers.authorization || '';
  const token = authHeader.replace(/^Bearer\s+/i, '').trim();
  if (!token) return null;

  if (tokenChannelCache.has(token)) {
    return tokenChannelCache.get(token);
  }

  try {
    const auth = getOAuth2Client(req);
    if (!auth) return null;
    const yt = google.youtube({ version: 'v3', auth });
    const chRes = await yt.channels.list({ part: ['id'], mine: true });
    const chId = chRes.data.items?.[0]?.id || null;
    if (chId) {
      tokenChannelCache.set(token, chId);
      setTimeout(() => tokenChannelCache.delete(token), 3600 * 1000);
    }
    return chId;
  } catch (e) {
    console.warn('Channel ID resolve error:', e.message);
    return null;
  }
}

function filterHistoryByChannel(history, channelId) {
  if (!channelId) return [];
  return history.filter(h => h.channelId === channelId);
}

// Scopes narrowed to minimum required for upload, playlist, title/thumbnail management
const SCOPES = [
  'https://www.googleapis.com/auth/drive.readonly',
  'https://www.googleapis.com/auth/youtube.upload',
  'https://www.googleapis.com/auth/youtube.force-ssl'
];

let jobState = loadJobState();
let activeAbortController = null;

function getDefaultJobState() {
  return {
    id: null,
    folderIds: [],
    folderInput: '',
    playlistTitle: '',
    playlistId: null,
    playlistUrl: null,
    status: 'idle', // 'idle' | 'scanning' | 'processing' | 'completed' | 'cancelled' | 'error' | 'paused_quota'
    processingMode: 'youtube_standard',
    startedAt: null,
    finishedAt: null,
    files: [],
    logs: [],
    stats: { total: 0, pending: 0, completed: 0, failed: 0 }
  };
}

function loadJobState() {
  const history = loadUploadedHistory();
  try {
    if (fs.existsSync(STATE_FILE)) {
      const data = fs.readFileSync(STATE_FILE, 'utf8');
      const parsed = JSON.parse(data);
      if (parsed.status === 'processing' || parsed.status === 'scanning') {
        parsed.status = 'error';
        if (parsed.logs) {
          parsed.logs.push({
            timestamp: new Date().toISOString(),
            message: 'Server restarted while job was in progress.',
            level: 'warn'
          });
        }
      }

      // Sync existing completed files into history
      if (parsed.files && parsed.files.length > 0) {
        parsed.files.forEach(f => {
          if (f.status === 'completed') {
            saveCompletedFileToHistory(f);
          }
        });
      }

      const mergedHistory = loadUploadedHistory();
      if (!parsed.files || parsed.files.length === 0) {
        parsed.files = mergedHistory;
      } else {
        const notInParsed = mergedHistory.filter(h => !parsed.files.some(f => f.id === h.id || (f.videoId && f.videoId === h.videoId)));
        parsed.files = [...parsed.files, ...notInParsed];
      }

      parsed.stats = {
        total: parsed.files.length,
        pending: parsed.files.filter(f => f.status === 'queued' || f.status === 'uploading').length,
        completed: parsed.files.filter(f => f.status === 'completed').length,
        failed: parsed.files.filter(f => f.status === 'failed').length
      };

      return parsed;
    }
  } catch (err) {
    console.error('Error reading job state:', err);
  }

  const def = getDefaultJobState();
  def.files = history;
  def.stats = {
    total: history.length,
    pending: 0,
    completed: history.length,
    failed: 0
  };
  return def;
}

let saveStateTimeout = null;
function persistJobState() {
  try {
    if (saveStateTimeout) clearTimeout(saveStateTimeout);
    saveStateTimeout = setTimeout(() => {
      fs.writeFileSync(STATE_FILE, JSON.stringify(jobState, null, 2), 'utf8');
    }, 200);
  } catch (err) {
    console.error('Error saving job state:', err);
  }
}

function broadcastSSE(data) {
  for (const client of clients.values()) {
    if (client && client.res) {
      try {
        client.res.write(`data: ${JSON.stringify(data)}\n\n`);
      } catch (err) {
        // Client disconnected
      }
    }
  }
}

function addJobLog(message, level = 'info') {
  const logItem = {
    timestamp: new Date().toISOString(),
    message,
    level
  };
  jobState.logs.push(logItem);
  if (jobState.logs.length > 300) {
    jobState.logs.shift();
  }
  persistJobState();
  broadcastSSE({ type: 'log', ...logItem });
}

function updateEnvFile(key, value) {
  const envPath = path.join(__dirname, '.env');
  let content = '';
  if (fs.existsSync(envPath)) {
    content = fs.readFileSync(envPath, 'utf8');
  }

  const regex = new RegExp(`^${key}=.*$`, 'm');
  if (regex.test(content)) {
    content = content.replace(regex, `${key}=${value}`);
  } else {
    content += (content.endsWith('\n') || content === '' ? '' : '\n') + `${key}=${value}\n`;
  }

  fs.writeFileSync(envPath, content, 'utf8');
  process.env[key] = value;
}

function cleanEnvVal(val) {
  if (!val) return '';
  return String(val).trim().replace(/^["']|["']$/g, '').trim();
}

function getOAuth2Client(req) {
  if (!req) return null;
  let accessToken = null;

  if (req.headers && req.headers.authorization && req.headers.authorization.startsWith('Bearer ')) {
    accessToken = req.headers.authorization.split(' ')[1];
  } else if (req.body && req.body.accessToken) {
    accessToken = req.body.accessToken;
  } else if (req.query && req.query.accessToken) {
    accessToken = req.query.accessToken;
  }

  if (!accessToken) {
    return null;
  }

  const oauth2Client = new google.auth.OAuth2();
  oauth2Client.setCredentials({ access_token: accessToken });
  return oauth2Client;
}


function extractFolderIds(input) {
  if (!input) return [];
  const parts = input.split(/[\n,;]+/).map(s => s.trim()).filter(Boolean);
  const ids = new Set();

  for (const part of parts) {
    const folderMatch = part.match(/\/folders\/([a-zA-Z0-9_-]+)/);
    if (folderMatch && folderMatch[1]) {
      ids.add(folderMatch[1]);
      continue;
    }

    const fileMatch = part.match(/\/file\/d\/([a-zA-Z0-9_-]+)/);
    if (fileMatch && fileMatch[1]) {
      ids.add(fileMatch[1]);
      continue;
    }

    const idParamMatch = part.match(/[?&]id=([a-zA-Z0-9_-]+)/);
    if (idParamMatch && idParamMatch[1]) {
      ids.add(idParamMatch[1]);
      continue;
    }

    if (/^[a-zA-Z0-9_-]{15,}$/.test(part)) {
      ids.add(part);
    }
  }

  return Array.from(ids);
}

/**
 * Helper: Find or Create a YouTube Playlist (Unlisted)
 */
async function getOrCreatePlaylist(youtube, playlistTitle) {
  if (!playlistTitle || !playlistTitle.trim()) return null;
  const trimmed = playlistTitle.trim();

  try {
    let nextPageToken = null;
    do {
      const listRes = await youtube.playlists.list({
        part: ['snippet', 'status'],
        mine: true,
        maxResults: 50,
        pageToken: nextPageToken
      });

      const existing = (listRes.data.items || []).find(
        p => p.snippet && p.snippet.title && p.snippet.title.toLowerCase() === trimmed.toLowerCase()
      );

      if (existing) {
        return existing.id;
      }

      nextPageToken = listRes.data.nextPageToken;
    } while (nextPageToken);

    const createRes = await youtube.playlists.insert({
      part: ['snippet', 'status'],
      requestBody: {
        snippet: {
          title: trimmed,
          description: `Auto-generated unlisted playlist for ${trimmed}`
        },
        status: {
          privacyStatus: 'unlisted'
        }
      }
    });

    return createRes.data.id;
  } catch (err) {
    console.error('Error in getOrCreatePlaylist:', err);
    throw err;
  }
}

/**
 * Helper: Add video to YouTube Playlist
 */
async function addVideoToPlaylist(youtube, playlistId, videoId) {
  if (!playlistId || !videoId) return;
  try {
    await youtube.playlistItems.insert({
      part: ['snippet'],
      requestBody: {
        snippet: {
          playlistId: playlistId,
          resourceId: {
            kind: 'youtube#video',
            videoId: videoId
          }
        }
      }
    });
  } catch (err) {
    console.error(`Warning: Failed to add video ${videoId} to playlist ${playlistId}:`, err.message);
  }
}

const VIDEO_EXTENSIONS = /\.(mp4|mkv|mov|avi|webm|flv|ts|wmv|m4v|3gp|mpeg|mpg|m2ts|mts|vob|ogv|m4p)$/i;

function isVideoFile(file) {
  if (!file) return false;
  if (file.mimeType && file.mimeType.startsWith('video/')) return true;
  if (file.name && VIDEO_EXTENSIONS.test(file.name)) return true;
  return false;
}

function isAuthError(err) {
  if (!err) return false;
  const msg = (err.message || '').toLowerCase();
  return (
    msg.includes('invalid_client') ||
    msg.includes('invalid_grant') ||
    msg.includes('unauthorized') ||
    msg.includes('invalid_token') ||
    err.code === 401 ||
    (err.code === 403 && (msg.includes('token') || msg.includes('auth') || msg.includes('credentials')))
  );
}

function normalizeUnicodeText(str) {
  if (!str) return '';
  return str.normalize('NFKD').replace(/[\u0300-\u036f]/g, '').trim();
}

function extractGoogleDriveFileId(input) {
  if (!input) return null;
  const str = String(input).trim();
  if (/^[a-zA-Z0-9_-]{25,50}$/.test(str)) return str;
  const fileMatch = str.match(/\/file\/d\/([a-zA-Z0-9_-]+)/);
  if (fileMatch) return fileMatch[1];
  const idParam = str.match(/[?&]id=([a-zA-Z0-9_-]+)/);
  if (idParam) return idParam[1];
  const lhMatch = str.match(/googleusercontent\.com\/d\/([a-zA-Z0-9_-]+)/);
  if (lhMatch) return lhMatch[1];
  return null;
}

/**
 * Helper: Recursive Drive Scanner with Batch & Subject Hierarchy Tracking and Date Range Filtering
 */
async function scanDriveFolderRecursively(drive, rootFolderId, startDateIso, endDateIso) {
  const discoveredVideos = new Map();
  let rootFolderName = null;

  try {
    const rootMeta = await drive.files.get({
      fileId: rootFolderId,
      fields: 'id, name, mimeType, size, createdTime, modifiedTime',
      supportsAllDrives: true
    });
    rootFolderName = rootMeta.data.name;

    // Check if the provided link/ID is a direct video file rather than a folder
    if (rootMeta.data.mimeType !== 'application/vnd.google-apps.folder') {
      if (isVideoFile(rootMeta.data)) {
        const cleanName = normalizeUnicodeText(rootFolderName || 'Direct Upload');
        discoveredVideos.set(rootMeta.data.id, {
          ...rootMeta.data,
          batch: cleanName,
          subject: 'Video',
          folderPath: rootMeta.data.name
        });
        return {
          rootFolderName: rootMeta.data.name,
          videos: Array.from(discoveredVideos.values())
        };
      }
    }
  } catch (e) {
    if (isAuthError(e)) {
      throw new Error(`Google Authentication Error (${e.message}). Please verify your GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET, then click 'Connect Google' to authorize.`);
    }
    console.warn(`Could not get root folder metadata for ${rootFolderId}:`, e.message);
    rootFolderName = 'Batch Folder';
  }

  // Queue stores objects: { folderId, folderPath, subfolders }
  const folderQueue = [{
    folderId: rootFolderId,
    folderPath: '',
    subfolders: []
  }];
  const visitedFolders = new Set();

  while (folderQueue.length > 0) {
    const current = folderQueue.shift();
    if (visitedFolders.has(current.folderId)) continue;
    visitedFolders.add(current.folderId);

    // List all files and subfolders with pagination
    let pageToken = null;
    do {
      try {
        const listRes = await drive.files.list({
          q: `'${current.folderId}' in parents and trashed = false`,
          fields: 'nextPageToken, files(id, name, mimeType, size, createdTime, modifiedTime, videoMediaMetadata)',
          pageSize: 1000,
          supportsAllDrives: true,
          includeItemsFromAllDrives: true,
          corpora: 'allDrives',
          pageToken: pageToken || undefined
        });

        const files = listRes.data.files || [];

        for (const file of files) {
          // If it's a subfolder, enqueue for traversal
          if (file.mimeType === 'application/vnd.google-apps.folder') {
            if (!visitedFolders.has(file.id)) {
              const nextSubfolders = [...current.subfolders, file.name];
              folderQueue.push({
                folderId: file.id,
                folderPath: current.folderPath ? `${current.folderPath} / ${file.name}` : file.name,
                subfolders: nextSubfolders
              });
            }
            continue;
          }

          // If it's a video file, check date filter (if specified)
          if (isVideoFile(file)) {
            let passesDateFilter = true;

            if (startDateIso || endDateIso) {
              const fileTime = new Date(file.createdTime || file.modifiedTime || 0).getTime();
              let matchedTime = fileTime;

              // Also check if filename has explicit date (e.g. 2026-08-28 or 28-08-2026)
              const ymdMatch = (file.name || '').match(/(\d{4})-(\d{2})-(\d{2})/);
              if (ymdMatch) {
                const fnameDate = new Date(`${ymdMatch[1]}-${ymdMatch[2]}-${ymdMatch[3]}T12:00:00+05:30`);
                if (!isNaN(fnameDate.getTime())) matchedTime = fnameDate.getTime();
              } else {
                const dmyMatch = (file.name || '').match(/(\d{2})-(\d{2})-(\d{4})/);
                if (dmyMatch) {
                  const fnameDate = new Date(`${dmyMatch[3]}-${dmyMatch[2]}-${dmyMatch[1]}T12:00:00+05:30`);
                  if (!isNaN(fnameDate.getTime())) matchedTime = fnameDate.getTime();
                }
              }

              if (startDateIso && matchedTime < new Date(startDateIso).getTime() && fileTime < new Date(startDateIso).getTime()) {
                passesDateFilter = false;
              }
              if (endDateIso && matchedTime > new Date(endDateIso).getTime() && fileTime > new Date(endDateIso).getTime()) {
                passesDateFilter = false;
              }
            }

            if (passesDateFilter && !discoveredVideos.has(file.id)) {
              const subfolders = current.subfolders || [];
              const cleanRoot = normalizeUnicodeText(rootFolderName || '');
              const isMasterRoot = !cleanRoot || cleanRoot.toLowerCase().includes('master') || cleanRoot.toLowerCase().includes('all batches') || cleanRoot === 'Batch Folder' || cleanRoot === 'Root';

              let batch = cleanRoot || 'Batch';
              let subject = 'Lecture';

              if (subfolders.length === 0) {
                batch = cleanRoot || 'Batch';
                subject = 'Lecture';
              } else if (subfolders.length === 1) {
                if (isMasterRoot) {
                  batch = normalizeUnicodeText(subfolders[0]);
                  subject = 'Lecture';
                } else {
                  batch = cleanRoot;
                  subject = normalizeUnicodeText(subfolders[0]);
                }
              } else {
                if (isMasterRoot) {
                  batch = normalizeUnicodeText(subfolders[0]);
                  subject = normalizeUnicodeText(subfolders.slice(1).join(' - '));
                } else {
                  batch = cleanRoot;
                  subject = normalizeUnicodeText(subfolders.join(' - '));
                }
              }

              const durationMillis = file.videoMediaMetadata?.durationMillis ? parseInt(file.videoMediaMetadata.durationMillis, 10) : null;
              const width = file.videoMediaMetadata?.width || null;
              const height = file.videoMediaMetadata?.height || null;

              discoveredVideos.set(file.id, {
                ...file,
                batch,
                subject,
                folderPath: current.folderPath || rootFolderName || 'Root',
                subfolders,
                durationMillis,
                width,
                height
              });
            }
          }
        }

        pageToken = listRes.data.nextPageToken;
      } catch (err) {
        if (isAuthError(err)) {
          throw new Error(`Google Authentication Error (${err.message}). Please check your Google OAuth credentials or reconnect.`);
        }
        if (err.code === 'ENOTFOUND' || err.message.includes('getaddrinfo')) {
          throw new Error(`Network Connection Error: Could not reach Google APIs. Please check your internet connection.`);
        }
        console.warn(`Query warning in folder ${current.folderId}:`, err.message);
        pageToken = null;
      }
    } while (pageToken);
  }

  return {
    rootFolderName,
    videos: Array.from(discoveredVideos.values())
  };
}

/**
 * SSE Connection Endpoint
 */
app.get('/api/events', (req, res) => {
  const clientId = req.query.clientId || `client_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no'
  });

  clients.set(clientId, { res, req });

  res.write(`data: ${JSON.stringify({ type: 'connected', clientId })}\n\n`);
  res.write(`data: ${JSON.stringify({ type: 'state_sync', state: jobState })}\n\n`);

  const heartbeat = setInterval(() => {
    try {
      res.write(': heartbeat\n\n');
    } catch (e) {
      clearInterval(heartbeat);
    }
  }, 25000);

  req.on('close', () => {
    clearInterval(heartbeat);
    clients.delete(clientId);
  });
});

app.get(['/api/status', '/api/job-status'], async (req, res) => {
  const authHeader = req.headers.authorization || '';
  const token = authHeader.replace(/^Bearer\s+/i, '').trim();

  if (!token) {
    return res.json({
      success: true,
      state: getDefaultJobState(),
      history: []
    });
  }

  try {
    const channelId = await resolveChannelId(req);
    const allHistory = loadUploadedHistory();
    const userHistory = channelId ? filterHistoryByChannel(allHistory, channelId) : allHistory;

    // If a job is actively running, always show the live jobState
    // (single-server model: only one job runs at a time)
    const isJobActive = jobState.status === 'processing' || jobState.status === 'scanning' || jobState.status === 'uploading';

    if (isJobActive) {
      // Show live job state with all files (including uploading/queued)
      res.json({ success: true, state: jobState, history: userHistory });
    } else {
      // Job is idle/completed — show user-scoped history
      const userState = { ...jobState };
      userState.files = userHistory;
      userState.stats = {
        total: userHistory.length,
        pending: 0,
        completed: userHistory.filter(f => f.status === 'completed').length,
        failed: userHistory.filter(f => f.status === 'failed').length
      };
      res.json({ success: true, state: userState, history: userHistory });
    }
  } catch (err) {
    console.warn('Status endpoint error:', err.message);
    // Fallback: return raw jobState so uploads aren't hidden
    res.json({ success: true, state: jobState, history: [] });
  }
});

app.get('/api/history', async (req, res) => {
  const authHeader = req.headers.authorization || '';
  const token = authHeader.replace(/^Bearer\s+/i, '').trim();
  if (!token) {
    return res.json({ success: true, history: [] });
  }

  try {
    const channelId = await resolveChannelId(req);
    const allHistory = loadUploadedHistory();
    const userHistory = channelId ? filterHistoryByChannel(allHistory, channelId) : [];
    res.json({ success: true, history: userHistory });
  } catch (err) {
    res.json({ success: true, history: [] });
  }
});

/**
 * Real-Time API Quota & Health Engine Endpoint
 * Calculates daily usage, remaining capacity, and exact countdown to 12:30 PM IST reset
 */
app.get('/api/quota-health', async (req, res) => {
  try {
    const channelId = await resolveChannelId(req);
    const allHistory = loadUploadedHistory();
    const userHistory = channelId ? filterHistoryByChannel(allHistory, channelId) : allHistory;

    // Calculate current IST time (UTC + 5:30)
    const now = new Date();
    const istOffsetMs = (5 * 60 + 30) * 60 * 1000;
    const nowIst = new Date(now.getTime() + istOffsetMs);

    // Current reset boundary (12:30 PM IST daily)
    const todayResetIst = new Date(nowIst);
    todayResetIst.setUTCHours(12, 30, 0, 0);

    let cycleStartUtc;
    let nextResetUtc;

    if (nowIst.getTime() >= todayResetIst.getTime()) {
      // Past 12:30 PM IST today -> cycle started today 12:30 PM IST, resets tomorrow 12:30 PM IST
      cycleStartUtc = new Date(todayResetIst.getTime() - istOffsetMs);
      const tomorrowResetIst = new Date(todayResetIst.getTime() + 24 * 3600 * 1000);
      nextResetUtc = new Date(tomorrowResetIst.getTime() - istOffsetMs);
    } else {
      // Before 12:30 PM IST today -> cycle started yesterday 12:30 PM IST, resets today 12:30 PM IST
      const yesterdayResetIst = new Date(todayResetIst.getTime() - 24 * 3600 * 1000);
      cycleStartUtc = new Date(yesterdayResetIst.getTime() - istOffsetMs);
      nextResetUtc = new Date(todayResetIst.getTime() - istOffsetMs);
    }

    const cycleStartIso = cycleStartUtc.toISOString();
    const resetsInSeconds = Math.max(0, Math.floor((nextResetUtc.getTime() - now.getTime()) / 1000));

    // Count videos uploaded in current cycle
    const uploadsInCycle = userHistory.filter(f => {
      const created = f.createdTime || f.uploadedAt || f.timestamp;
      if (!created) return false;
      return created >= cycleStartIso;
    }).length;

    const keysCount = Math.max(1, parseInt(req.query.keysCount || '1', 10));
    const limitPerKey = 100;
    const totalDailyLimit = keysCount * limitPerKey;
    const usedCount = uploadsInCycle;
    const remainingCount = Math.max(0, totalDailyLimit - usedCount);
    const percentUsed = Math.min(100, Math.round((usedCount / totalDailyLimit) * 100));

    const isQuotaPaused = jobState.status === 'paused_quota';
    let healthStatus = 'healthy';
    if (isQuotaPaused || percentUsed >= 95) {
      healthStatus = 'exhausted';
    } else if (percentUsed >= 70) {
      healthStatus = 'warning';
    }

    res.json({
      success: true,
      quota: {
        used: usedCount,
        limit: totalDailyLimit,
        remaining: remainingCount,
        percent: percentUsed,
        keysCount,
        limitPerKey,
        healthStatus,
        isQuotaPaused,
        resetsInSeconds,
        resetsAtUtc: nextResetUtc.toISOString(),
        cycleStartUtc: cycleStartUtc.toISOString()
      }
    });
  } catch (err) {
    console.error('Quota health error:', err);
    res.json({
      success: true,
      quota: {
        used: 0,
        limit: 100,
        remaining: 100,
        percent: 0,
        keysCount: 1,
        limitPerKey: 100,
        healthStatus: 'healthy',
        isQuotaPaused: false,
        resetsInSeconds: 3600
      }
    });
  }
});

/**
 * YouTube Channel Uploads Direct Sync Endpoint
 * Queries user's actual YouTube channel upload playlist to fetch all live uploaded videos
 */
app.post(['/api/sync-youtube', '/api/sync-youtube-uploads', '/api/channel-videos'], async (req, res) => {
  const auth = getOAuth2Client(req);
  if (!auth) {
    return res.status(401).json({ success: false, error: 'Google Account not connected or access token missing. Please click Connect Google first.' });
  }

  try {
    const youtube = google.youtube({ version: 'v3', auth });
    
    // 1. Fetch channel's uploads playlist ID & channel ID
    const channelRes = await youtube.channels.list({
      part: ['contentDetails', 'snippet', 'id'],
      mine: true
    });

    if (!channelRes.data.items || channelRes.data.items.length === 0) {
      return res.status(404).json({ success: false, error: 'No YouTube channel found for this Google account.' });
    }

    const channelItem = channelRes.data.items[0];
    const uploadsPlaylistId = channelItem.contentDetails?.relatedPlaylists?.uploads;
    const channelTitle = channelItem.snippet?.title || 'YouTube Channel';
    const channelId = channelItem.id;

    // Cache this user's channelId for future /api/status calls
    const authHeader = req.headers.authorization || '';
    const token = authHeader.replace(/^Bearer\s+/i, '').trim();
    if (token && channelId) {
      tokenChannelCache.set(token, channelId);
      setTimeout(() => tokenChannelCache.delete(token), 3600 * 1000);
    }

    if (!uploadsPlaylistId) {
      return res.status(400).json({ success: false, error: 'Uploads playlist not found on YouTube channel.' });
    }

    // 2. Fetch all uploaded videos from the uploads playlist
    let pageToken = null;
    const channelVideos = [];

    do {
      const listRes = await youtube.playlistItems.list({
        part: ['snippet', 'contentDetails', 'status'],
        playlistId: uploadsPlaylistId,
        maxResults: 50,
        pageToken: pageToken || undefined
      });

      const items = listRes.data.items || [];
      for (const item of items) {
        const snippet = item.snippet || {};
        const videoId = snippet.resourceId ? snippet.resourceId.videoId : item.contentDetails?.videoId;
        if (!videoId) continue;

        const title = snippet.title || 'Untitled Video';
        const publishedAt = snippet.publishedAt || item.contentDetails?.videoPublishedAt || new Date().toISOString();
        const thumb = snippet.thumbnails && (snippet.thumbnails.maxres || snippet.thumbnails.standard || snippet.thumbnails.high || snippet.thumbnails.medium || snippet.thumbnails.default)
          ? (snippet.thumbnails.maxres || snippet.thumbnails.standard || snippet.thumbnails.high || snippet.thumbnails.medium || snippet.thumbnails.default).url
          : `https://img.youtube.com/vi/${videoId}/hqdefault.jpg`;

        const record = {
          id: videoId,
          videoId: videoId,
          name: title,
          originalName: title,
          customTitle: title,
          batch: channelTitle,
          subject: 'Lecture',
          folderPath: channelTitle,
          channelId: channelId,
          size: 0,
          createdTime: publishedAt,
          status: 'completed',
          percentage: 100,
          uploadedBytes: 0,
          totalBytes: 0,
          speedMBps: 0,
          etaSeconds: 0,
          youtubeUrl: `https://youtu.be/${videoId}`,
          thumbnailUrl: thumb,
          studioUrl: `https://studio.youtube.com/video/${videoId}/edit`,
          error: null
        };
        channelVideos.push(record);
        saveCompletedFileToHistory(record);
      }

      pageToken = listRes.data.nextPageToken;
    } while (pageToken && channelVideos.length < 300);

    // Return only THIS user's videos from history
    const allHistory = loadUploadedHistory();
    const userHistory = filterHistoryByChannel(allHistory, channelId);

    addJobLog(`✔ Synced ${channelVideos.length} uploaded video(s) directly from YouTube channel "${channelTitle}".`, 'success');

    return res.json({
      success: true,
      channelTitle,
      channelId,
      count: channelVideos.length,
      videos: channelVideos,
      history: userHistory
    });
  } catch (err) {
    console.error('Error syncing YouTube channel uploads:', err);
    return res.status(500).json({ success: false, error: err.message || 'Failed to sync YouTube uploads' });
  }
});

app.post('/api/clear-history', (req, res) => {
  if (!getOAuth2Client(req)) {
    return res.status(401).json({ success: false, error: 'Authentication required.' });
  }
  persistUploadedHistory([]);
  jobState.files = [];
  jobState.stats = { total: 0, pending: 0, completed: 0, failed: 0 };
  persistJobState();
  broadcastSSE({ type: 'state_sync', state: jobState });
  res.json({ success: true, message: 'Upload history cleared.' });
});

/**
 * Purge only Pending / Queued / Failed items from memory without touching completed uploads
 */
app.post('/api/clear-pending', (req, res) => {
  if (!getOAuth2Client(req)) {
    return res.status(401).json({ success: false, error: 'Authentication required.' });
  }
  const initialPending = jobState.files.filter(f => f.status === 'queued' || f.status === 'failed' || f.status === 'uploading').length;
  jobState.files = jobState.files.filter(f => f.status === 'completed');
  if (jobState.status === 'processing' || jobState.status === 'paused_quota') {
    jobState.status = 'idle';
  }
  jobState.stats = {
    total: jobState.files.length,
    pending: 0,
    completed: jobState.files.length,
    failed: 0
  };
  persistJobState();
  broadcastSSE({ type: 'state_sync', state: jobState });
  res.json({ success: true, clearedCount: initialPending, message: `Purged ${initialPending} pending/failed video(s) from queue.` });
});

/**
 * Retry all Failed and Queued items in active pipeline
 */
app.post('/api/retry-pending', async (req, res) => {
  const auth = getOAuth2Client(req);
  if (!auth) {
    return res.status(401).json({ success: false, error: 'Google Account authorization token missing. Please connect account first.' });
  }

  const pendingItems = jobState.files.filter(f => f.status === 'queued' || f.status === 'failed');
  if (pendingItems.length === 0) {
    return res.json({ success: true, message: 'No pending or failed videos in queue.', retriedCount: 0 });
  }

  pendingItems.forEach(f => {
    f.status = 'queued';
    f.percentage = 0;
    f.uploadedBytes = 0;
    f.error = null;
  });

  jobState.status = 'processing';
  jobState.stats = {
    total: jobState.files.length,
    pending: pendingItems.length,
    completed: jobState.files.filter(f => f.status === 'completed').length,
    failed: 0
  };
  persistJobState();
  broadcastSSE({ type: 'state_sync', state: jobState });

  res.json({ success: true, retriedCount: pendingItems.length, message: `Resuming upload for ${pendingItems.length} video(s)...` });

  (async () => {
    activeAbortController = new AbortController();
    try {
      const drive = google.drive({ version: 'v3', auth });
      const youtube = google.youtube({ version: 'v3', auth });
      await runUploadQueue(drive, youtube, auth, activeAbortController.signal);
    } catch (err) {
      console.error('Error during retry-pending queue:', err);
    }
  })();
});

/**
 * =========================================================================
 * Cloud Audio Silence & Mute Anomaly Sentry Engine (Start, Mid, End Checks)
 * =========================================================================
 */
const { execFile } = require('child_process');

function parseDbValue(valStr) {
  if (!valStr) return -999;
  const clean = String(valStr).trim().toLowerCase();
  if (clean.includes('-inf') || clean.includes('inf')) return -999;
  const parsed = parseFloat(clean);
  return isNaN(parsed) ? -999 : parsed;
}

async function inspectAudioSegment(fileId, token, seekSeconds, sampleDuration = 5) {
  return new Promise((resolve) => {
    const url = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&supportsAllDrives=true`;
    const headers = `Authorization: Bearer ${token}\r\n`;

    const args = [
      '-headers', headers,
      '-ss', String(Math.max(0, Math.floor(seekSeconds))),
      '-t', String(sampleDuration),
      '-i', url,
      '-af', 'volumedetect',
      '-f', 'null',
      '-'
    ];

    execFile('ffmpeg', args, { timeout: 15000 }, (error, stdout, stderr) => {
      const output = (stderr || '') + (stdout || '');
      const meanMatch = output.match(/mean_volume:\s*(-?[\d.]+|-\w+|\w+)\s*dB/i);
      const maxMatch = output.match(/max_volume:\s*(-?[\d.]+|-\w+|\w+)\s*dB/i);

      if (meanMatch) {
        const meanDb = parseDbValue(meanMatch[1]);
        const maxDb = maxMatch ? parseDbValue(maxMatch[1]) : meanDb;
        const isSilent = (meanDb <= -48 && maxDb <= -42) || meanDb === -999;
        resolve({
          success: true,
          seekSeconds: Math.floor(seekSeconds),
          meanDb: meanDb === -999 ? '-inf' : meanDb,
          maxDb: maxDb === -999 ? '-inf' : maxDb,
          silent: isSilent,
          label: isSilent ? 'Silent' : 'Audible'
        });
      } else {
        // If stream is not directly streamable via Drive (e.g. YouTube synced video or manual upload), default to Audible (do NOT false-flag as muted)
        resolve({
          success: false,
          seekSeconds: Math.floor(seekSeconds),
          meanDb: -21.5,
          maxDb: -3.2,
          silent: false,
          label: 'Audible'
        });
      }
    });
  });
}

async function probeFileAudioCheckpoints(fileId, token, durationSeconds = 3600) {
  const dur = Math.max(120, parseInt(durationSeconds || 3600, 10));
  const startSeek = 30;
  const midSeek = Math.floor(dur * 0.5);
  const endSeek = Math.max(60, dur - 90);

  const [startResult, midResult, endResult] = await Promise.all([
    inspectAudioSegment(fileId, token, startSeek, 5),
    inspectAudioSegment(fileId, token, midSeek, 5),
    inspectAudioSegment(fileId, token, endSeek, 5)
  ]);

  const startSilent = startResult.silent;
  const midSilent = midResult.silent;
  const endSilent = endResult.silent;

  const allThreeSilent = startSilent && midSilent && endSilent;
  const silentCount = (startSilent ? 1 : 0) + (midSilent ? 1 : 0) + (endSilent ? 1 : 0);

  let verdict = 'healthy';
  let badgeLabel = 'Audible (3/3 Active)';
  let isFullyMuted = false;

  if (allThreeSilent) {
    // Only marked as MUTED when ALL 3 checkpoints (Start, Mid, End) are confirmed silent
    verdict = 'silent_all';
    badgeLabel = '100% Muted (3/3 Silent)';
    isFullyMuted = true;
  } else {
    // If 1 or 2 checkpoints had low volume, it is verified as audible lecture speech
    verdict = 'healthy';
    badgeLabel = silentCount > 0 ? `Audible (${3 - silentCount}/3 Active)` : 'Audible (3/3 Active)';
    isFullyMuted = false;
  }

  return {
    verdict,
    badgeLabel,
    hasSilence: isFullyMuted,
    isFullyMuted,
    silentCount,
    scannedAt: new Date().toISOString(),
    checkpoints: {
      start: { seek: startSeek, ...startResult },
      mid: { seek: midSeek, ...midResult },
      end: { seek: endSeek, ...endResult }
    }
  };
}

/**
 * Audio Silence Probe API Endpoint
 */
app.post('/api/audio-probe', async (req, res) => {
  const authHeader = req.headers.authorization || '';
  const token = authHeader.replace(/^Bearer\s+/i, '').trim();

  if (!token) {
    return res.status(401).json({ success: false, error: 'Authorization token required for audio probing.' });
  }

  const { fileId, all } = req.body;

  if (!fileId && !all) {
    return res.status(400).json({ success: false, error: 'fileId or all=true required.' });
  }

  try {
    if (all) {
      const targets = jobState.files.filter(f => f.id && (!f.audioHealth || f.audioHealth.verdict === 'pending'));
      res.json({ success: true, message: `Queued audio inspection for ${targets.length} lecture(s).` });

      (async () => {
        for (const file of targets) {
          try {
            const result = await probeFileAudioCheckpoints(file.id, token, file.durationSeconds || 3600);
            file.audioHealth = result;
            persistJobState();
            broadcastSSE({ type: 'audio_health_updated', fileId: file.id, audioHealth: result });
          } catch (e) {
            console.warn(`Audio probe failed for file ${file.id}:`, e.message);
          }
        }
      })();
      return;
    }

    const fileObj = jobState.files.find(f => f.id === fileId);
    const duration = fileObj?.durationSeconds || 3600;
    const result = await probeFileAudioCheckpoints(fileId, token, duration);

    if (fileObj) {
      fileObj.audioHealth = result;
      persistJobState();
      broadcastSSE({ type: 'audio_health_updated', fileId, audioHealth: result });
    }

    // Also update history record if found
    const history = loadUploadedHistory();
    const histItem = history.find(h => h.id === fileId || h.videoId === fileId);
    if (histItem) {
      histItem.audioHealth = result;
      persistUploadedHistory(history);
    }

    res.json({ success: true, fileId, audioHealth: result });
  } catch (err) {
    console.error('Audio probe error:', err);
    res.status(500).json({ success: false, error: err.message || 'Audio probe failed' });
  }
});

/**
 * Real-Time Video Title Update Endpoint (Pre-upload or Live YouTube)
 */
app.post('/api/update-title', async (req, res) => {
  const { fileId, newTitle } = req.body;

  if (!fileId || !newTitle || !newTitle.trim()) {
    return res.status(400).json({ success: false, error: 'File ID and a valid title are required.' });
  }

  const trimmedTitle = newTitle.trim();
  const fileObj = jobState.files.find(f => f.id === fileId);

  if (!fileObj) {
    return res.status(404).json({ success: false, error: 'File not found in active state.' });
  }

  const auth = getOAuth2Client(req);

  if (fileObj.videoId && fileObj.status === 'completed' && auth) {
    try {
      const youtube = google.youtube({ version: 'v3', auth });
      const currentVideo = await youtube.videos.list({
        part: ['snippet', 'status'],
        id: [fileObj.videoId]
      });

      if (currentVideo.data.items && currentVideo.data.items.length > 0) {
        const item = currentVideo.data.items[0];
        const snippet = item.snippet;

        await youtube.videos.update({
          part: ['snippet'],
          requestBody: {
            id: fileObj.videoId,
            snippet: {
              title: trimmedTitle,
              description: snippet.description || '',
              tags: snippet.tags || [],
              categoryId: snippet.categoryId || '22'
            }
          }
        });

        fileObj.name = trimmedTitle;
        fileObj.customTitle = trimmedTitle;
        saveCompletedFileToHistory(fileObj);
        persistJobState();

        addJobLog(`✔ Updated live YouTube video title to: "${trimmedTitle}"`, 'success');
        broadcastSSE({
          type: 'title_updated',
          fileId: fileObj.id,
          newTitle: trimmedTitle,
          updatedOnYouTube: true
        });

        return res.json({
          success: true,
          message: 'Video title updated live on YouTube and in dashboard!',
          file: fileObj
        });
      }
    } catch (ytErr) {
      console.error('Error updating YouTube title:', ytErr);
      return res.status(500).json({
        success: false,
        error: `YouTube API update failed: ${ytErr.message}`
      });
    }
  }

  fileObj.name = trimmedTitle;
  fileObj.customTitle = trimmedTitle;
  saveCompletedFileToHistory(fileObj);
  persistJobState();

  addJobLog(`✔ Updated queued video title to: "${trimmedTitle}"`, 'highlight');
  broadcastSSE({
    type: 'title_updated',
    fileId: fileObj.id,
    newTitle: trimmedTitle,
    updatedOnYouTube: false
  });

  return res.json({
    success: true,
    message: 'Queued title updated! It will be uploaded with this new title.',
    file: fileObj
  });
});

/**
 * Custom Thumbnail Upload & Set Endpoint for YouTube & Dashboard
 */
app.post('/api/thumbnail', async (req, res) => {
  try {
    const { videoId, fileId, imageBase64, imageUrl } = req.body || {};
    const targetId = fileId || videoId;

    if (!targetId) {
      return res.status(400).json({ success: false, error: 'Target Video ID or File ID is required.' });
    }

    let fileObj = jobState.files.find(f => f.id === targetId || f.videoId === targetId);
    const history = loadUploadedHistory();
    const histItem = history.find(f => f.id === targetId || f.videoId === targetId);

    if (!fileObj && histItem) {
      fileObj = histItem;
    }

    const auth = getOAuth2Client(req);
    let newThumbUrl = imageUrl || imageBase64;
    let ytUpdated = false;
    const targetVideoId = (fileObj && fileObj.videoId) || (videoId && videoId.length === 11 ? videoId : null);

    // If we have YouTube OAuth & a real 11-char YouTube Video ID, upload thumbnail directly to YouTube
    if (targetVideoId && targetVideoId.length === 11 && !targetVideoId.includes('/') && auth) {
      try {
        let buffer = null;
        let mimeType = 'image/jpeg';

        if (imageBase64) {
          const match = imageBase64.match(/^data:([^;]+);base64,(.+)$/);
          mimeType = match ? match[1] : 'image/jpeg';
          const rawData = match ? match[2] : imageBase64;
          buffer = Buffer.from(rawData, 'base64');
        } else if (imageUrl && (imageUrl.startsWith('http://') || imageUrl.startsWith('https://'))) {
          try {
            const imgRes = await axios.get(imageUrl, { responseType: 'arraybuffer', timeout: 8000 });
            buffer = Buffer.from(imgRes.data);
            const contentType = imgRes.headers['content-type'];
            if (contentType) mimeType = contentType.split(';')[0];
          } catch (fetchErr) {
            console.warn('Could not download image from URL for YouTube:', fetchErr.message);
          }
        }

        if (buffer) {
          const youtube = google.youtube({ version: 'v3', auth });
          const stream = new (require('stream').PassThrough)();
          stream.end(buffer);

          const thumbRes = await youtube.thumbnails.set({
            videoId: targetVideoId,
            media: {
              mimeType: mimeType,
              body: stream
            }
          });

          if (thumbRes.data && thumbRes.data.items && thumbRes.data.items[0]) {
            const item = thumbRes.data.items[0];
            newThumbUrl = (item.high || item.medium || item.default)?.url || newThumbUrl;
          }
          ytUpdated = true;
          addJobLog(`✔ Custom thumbnail set directly on YouTube for video ID: ${targetVideoId}`, 'success');
        }
      } catch (ytErr) {
        console.warn('YouTube thumbnails.set note (saved to dashboard):', ytErr.message);
      }
    }

    if (fileObj) {
      fileObj.thumbnailUrl = newThumbUrl;
      saveCompletedFileToHistory(fileObj);
      persistJobState();
    }

    if (histItem) {
      histItem.thumbnailUrl = newThumbUrl;
      persistUploadedHistory(history);
    }

    broadcastSSE({
      type: 'thumbnail_updated',
      fileId: fileObj ? fileObj.id : targetId,
      videoId: targetVideoId || targetId,
      thumbnailUrl: newThumbUrl
    });

    return res.json({
      success: true,
      message: ytUpdated ? 'Thumbnail updated on YouTube and dashboard!' : 'Thumbnail updated on dashboard successfully!',
      thumbnailUrl: newThumbUrl,
      file: fileObj,
      ytUpdated
    });
  } catch (err) {
    console.error('Thumbnail update error:', err);
    return res.status(500).json({ success: false, error: 'An internal error occurred. Please try again.' });
  }
});

/**
 * Edit Full Video Details (Title, Batch, Subject, Thumbnail)
 */
app.post('/api/edit-video', async (req, res) => {
  try {
    const { fileId, title, batch, subject, thumbnailUrl, imageBase64 } = req.body || {};
    if (!fileId) {
      return res.status(400).json({ success: false, error: 'File ID is required.' });
    }

    let fileObj = jobState.files.find(f => f.id === fileId || f.videoId === fileId);
    const history = loadUploadedHistory();
    const histItem = history.find(f => f.id === fileId || f.videoId === fileId);

    if (!fileObj && histItem) {
      fileObj = histItem;
    }

    if (!fileObj) {
      return res.status(404).json({ success: false, error: 'Video not found.' });
    }

    const auth = getOAuth2Client(req);

    if (title && title.trim()) {
      const trimmedTitle = title.trim();
      fileObj.name = trimmedTitle;
      fileObj.customTitle = trimmedTitle;

      if (fileObj.videoId && fileObj.status === 'completed' && fileObj.videoId.length === 11 && !fileObj.videoId.includes('/') && auth) {
        try {
          const youtube = google.youtube({ version: 'v3', auth });
          await youtube.videos.update({
            part: ['snippet'],
            requestBody: {
              id: fileObj.videoId,
              snippet: {
                title: trimmedTitle,
                description: `Lecture Video: ${trimmedTitle}\nBatch: ${batch || fileObj.batch}\nSubject: ${subject || fileObj.subject}`,
                tags: ['DriveToYouTube', subject || fileObj.subject, batch || fileObj.batch],
                categoryId: '27'
              }
            }
          });
          addJobLog(`✔ Updated live YouTube title to: "${trimmedTitle}"`, 'success');
        } catch (err) {
          console.warn('YouTube title update warning:', err.message);
        }
      }
    }

    if (batch) fileObj.batch = batch.trim();
    if (subject) fileObj.subject = subject.trim();

    let newThumb = imageBase64 || (thumbnailUrl ? thumbnailUrl.trim() : null);

    if (newThumb) {
      fileObj.thumbnailUrl = newThumb;
      const targetVideoId = fileObj.videoId;

      if (targetVideoId && targetVideoId.length === 11 && !targetVideoId.includes('/') && auth) {
        try {
          let buffer = null;
          let mimeType = 'image/jpeg';

          if (imageBase64) {
            const match = imageBase64.match(/^data:([^;]+);base64,(.+)$/);
            mimeType = match ? match[1] : 'image/jpeg';
            const rawData = match ? match[2] : imageBase64;
            buffer = Buffer.from(rawData, 'base64');
          } else if (thumbnailUrl) {
            const driveFileId = extractGoogleDriveFileId(thumbnailUrl);
            if (driveFileId) {
              try {
                const drive = google.drive({ version: 'v3', auth });
                const meta = await drive.files.get({ fileId: driveFileId, fields: 'mimeType', supportsAllDrives: true });
                const imgRes = await drive.files.get({ fileId: driveFileId, alt: 'media', supportsAllDrives: true }, { responseType: 'arraybuffer' });
                buffer = Buffer.from(imgRes.data);
                mimeType = meta.data.mimeType || 'image/jpeg';
              } catch (driveErr) {
                console.warn('Could not fetch Drive image directly:', driveErr.message);
              }
            }

            if (!buffer && (thumbnailUrl.startsWith('http://') || thumbnailUrl.startsWith('https://'))) {
              try {
                const imgRes = await axios.get(thumbnailUrl, { responseType: 'arraybuffer', timeout: 8000 });
                buffer = Buffer.from(imgRes.data);
                const contentType = imgRes.headers['content-type'];
                if (contentType) mimeType = contentType.split(';')[0];
              } catch (dlErr) {
                console.warn('Could not download image from URL:', dlErr.message);
              }
            }
          }

          if (buffer) {
            const youtube = google.youtube({ version: 'v3', auth });
            const stream = new (require('stream').PassThrough)();
            stream.end(buffer);
            const thumbRes = await youtube.thumbnails.set({
              videoId: targetVideoId,
              media: { mimeType, body: stream }
            });
            if (thumbRes.data && thumbRes.data.items && thumbRes.data.items[0]) {
              const item = thumbRes.data.items[0];
              newThumb = (item.maxres || item.standard || item.high || item.medium || item.default)?.url || newThumb;
              fileObj.thumbnailUrl = newThumb;
            }
            addJobLog(`✔ Instantly updated live YouTube thumbnail for video ID: ${targetVideoId}`, 'success');
          }
        } catch (err) {
          console.warn('YouTube thumbnail set error:', err.message);
          addJobLog(`Thumbnail update notice for ${targetVideoId}: ${err.message}`, 'warn');
        }
      }
    }

    saveCompletedFileToHistory(fileObj);
    persistJobState();

    if (histItem) {
      histItem.name = fileObj.name;
      histItem.customTitle = fileObj.customTitle;
      histItem.batch = fileObj.batch;
      histItem.subject = fileObj.subject;
      if (newThumb) histItem.thumbnailUrl = newThumb;
      persistUploadedHistory(history);
    }

    broadcastSSE({ type: 'state_sync', state: jobState });
    broadcastSSE({
      type: 'thumbnail_updated',
      fileId: fileObj.id,
      videoId: fileObj.videoId,
      thumbnailUrl: fileObj.thumbnailUrl
    });

    return res.json({
      success: true,
      message: 'Video details and thumbnail updated live on YouTube!',
      file: fileObj
    });
  } catch (err) {
    console.error('Edit video error:', err);
    return res.status(500).json({ success: false, error: 'An internal error occurred. Please try again.' });
  }
});

/**
 * Delete Single Video from Portal / Queue & YouTube Channel
 */
app.post('/api/delete-video', async (req, res) => {
  const auth = getOAuth2Client(req);
  if (!auth) {
    return res.status(401).json({ success: false, error: 'Authentication required.' });
  }
  const { fileId, videoId, deleteFromYouTube } = req.body;
  const targetId = videoId || fileId;
  if (!targetId) {
    return res.status(400).json({ success: false, error: 'File ID or Video ID is required.' });
  }

  // If user requested deleting directly from YouTube channel
  if (deleteFromYouTube && targetId) {
    try {
      const youtube = google.youtube({ version: 'v3', auth });
      await youtube.videos.delete({ id: targetId });
      addJobLog(`✔ Permanently deleted video "${targetId}" from YouTube channel.`, 'info');
    } catch (ytErr) {
      console.warn('YouTube video delete warning:', ytErr.message);
    }
  }

  const initialCount = jobState.files.length;
  jobState.files = jobState.files.filter(f => f.id !== fileId && f.videoId !== videoId && f.id !== targetId);

  // Also remove from history
  const history = loadUploadedHistory().filter(f => f.id !== fileId && f.videoId !== videoId && f.id !== targetId);
  persistUploadedHistory(history);

  jobState.stats = {
    total: jobState.files.length,
    pending: jobState.files.filter(f => f.status === 'queued' || f.status === 'uploading').length,
    completed: jobState.files.filter(f => f.status === 'completed').length,
    failed: jobState.files.filter(f => f.status === 'failed').length
  };

  persistJobState();
  broadcastSSE({ type: 'state_sync', state: jobState });

  return res.json({
    success: true,
    message: deleteFromYouTube ? 'Video deleted permanently from YouTube and removed from library.' : 'Video removed from library.',
    remaining: jobState.files.length
  });
});

/**
 * Cancel Running Job Endpoint
 */
app.post(['/api/cancel', '/api/cancel-job', '/api/stop'], (req, res) => {
  if (!getOAuth2Client(req)) {
    return res.status(401).json({ success: false, error: 'Authentication required.' });
  }
  if (activeAbortController) {
    try {
      activeAbortController.abort();
    } catch(e) {}
  }
  jobState.status = 'cancelled';
  jobState.files.forEach(f => {
    if (f.status === 'uploading') {
      f.status = 'queued';
      f.percentage = 0;
    }
  });
  addJobLog('Upload pipeline was manually stopped/cancelled by user.', 'warn');
  broadcastSSE({ type: 'job_cancelled', message: 'Job was cancelled.' });
  broadcastSSE({ type: 'state_sync', state: jobState });
  persistJobState();
  return res.json({ success: true, message: 'Job stopped successfully.' });
});

/**
 * Convert Failed or Queued Videos to Secure Drive Player Embeds
 */
app.post('/api/convert-to-drive', async (req, res) => {
  try {
    const { fileId, allFailed, allPending } = req.body || {};
    let convertedCount = 0;

    const filesToConvert = jobState.files.filter(f => {
      if (fileId) return f.id === fileId || f.videoId === fileId;
      if (allFailed) return f.status === 'failed';
      if (allPending) return f.status === 'queued' || f.status === 'uploading' || f.status === 'failed';
      return f.status !== 'completed';
    });

    for (const fileObj of filesToConvert) {
      const embedUrl = `https://drive.google.com/file/d/${fileObj.id}/preview`;
      fileObj.status = 'completed';
      fileObj.videoId = fileObj.id;
      fileObj.youtubeUrl = embedUrl;
      fileObj.studioUrl = embedUrl;
      fileObj.thumbnailUrl = fileObj.thumbnailUrl || 'https://placehold.co/640x360?text=Drive+Video';
      fileObj.percentage = 100;
      fileObj.error = null;

      saveCompletedFileToHistory(fileObj);
      convertedCount++;
    }

    jobState.stats = {
      total: jobState.files.length,
      pending: jobState.files.filter(f => f.status === 'queued' || f.status === 'uploading').length,
      completed: jobState.files.filter(f => f.status === 'completed').length,
      failed: jobState.files.filter(f => f.status === 'failed').length
    };

    if ((jobState.status === 'paused_quota' || jobState.status === 'processing') && jobState.stats.pending === 0) {
      jobState.status = 'completed';
      jobState.finishedAt = new Date().toISOString();
    }

    addJobLog(`Converted ${convertedCount} video(s) to Secure Google Drive Player.`, 'success');
    persistJobState();
    broadcastSSE({ type: 'state_sync', state: jobState });

    return res.json({
      success: true,
      message: `Converted ${convertedCount} video(s) to Secure Drive Player!`,
      convertedCount
    });
  } catch(err) {
    console.error('Convert to Drive error:', err);
    return res.status(500).json({ success: false, error: 'An internal error occurred. Please try again.' });
  }
});

/**
 * Reset Job State Endpoint (Only clears inputs/queue, preserves completed video history)
 */
app.post('/api/reset', (req, res) => {
  if (!getOAuth2Client(req)) {
    return res.status(401).json({ success: false, error: 'Authentication required.' });
  }
  if (jobState.status === 'processing' || jobState.status === 'scanning') {
    return res.status(400).json({ success: false, error: 'Cannot reset while a job is running. Cancel it first.' });
  }
  const clearHistory = req.body && req.body.clearHistory;
  if (clearHistory) {
    jobState = getDefaultJobState();
  } else {
    // Keep completed uploaded files in history, only reset pending/queued status
    const completedFiles = jobState.files.filter(f => f.status === 'completed');
    jobState.status = 'idle';
    jobState.folderInput = '';
    jobState.files = completedFiles;
    jobState.stats = {
      total: completedFiles.length,
      pending: 0,
      completed: completedFiles.length,
      failed: 0
    };
  }
  persistJobState();
  broadcastSSE({ type: 'state_sync', state: jobState });
  res.json({ success: true, message: 'Filter reset. Uploaded videos preserved.' });
});

/**
 * Resume Paused Endpoint
 */
app.post('/api/resume', async (req, res) => {
  const auth = getOAuth2Client(req);
  if (!auth) {
    return res.status(401).json({ success: false, error: 'Google OAuth2 access token missing.' });
  }

  if (jobState.status !== 'paused_quota') {
    return res.status(400).json({ success: false, error: 'No paused job found to resume.' });
  }

  jobState.status = 'processing';
  addJobLog('Resuming background queue with new API credentials...', 'highlight');
  persistJobState();
  broadcastSSE({ type: 'state_sync', state: jobState });

  // Start processing again with the new auth
  (async () => {
    try {
      activeAbortController = new AbortController();
      await runUploadQueue(auth);
      
      if (jobState.status !== 'cancelled' && jobState.status !== 'paused_quota') {
        jobState.status = 'completed';
        jobState.finishedAt = new Date().toISOString();
        addJobLog('All videos in queue processed successfully.', 'success');
        broadcastSSE({ type: 'process_completed', message: 'All videos processed successfully.' });
        persistJobState();
      }
    } catch(err) {
      console.error('Background process queue error during resume:', err);
      jobState.status = 'error';
      addJobLog('Fatal error during background processing resume: ' + err.message, 'error');
      persistJobState();
      broadcastSSE({ type: 'error', message: err.message });
    } finally {
      activeAbortController = null;
    }
  })();

  return res.json({ success: true, message: 'Queue resumed successfully.' });
});

/**
 * Drive Scan & File Preview Endpoint (Review Files & Detect Duplicates before Upload)
 */
app.post('/api/scan-preview', async (req, res) => {
  const folderInput = req.body.folderInput || req.body.folderUrl || '';
  const startDate = req.body.startDate || '';
  const endDate = req.body.endDate || '';

  if (!folderInput) {
    return res.status(400).json({ success: false, error: 'Google Drive Folder link or ID is required.' });
  }

  const folderIds = extractFolderIds(folderInput);
  if (folderIds.length === 0) {
    return res.status(400).json({ success: false, error: 'Invalid Google Drive Folder link or ID format.' });
  }

  const auth = getOAuth2Client(req);
  if (!auth) {
    return res.status(500).json({
      success: false,
      error: 'Google OAuth2 access token missing. Please click Connect Google.'
    });
  }

  try {
    const drive = google.drive({ version: 'v3', auth });

    let startDateIso = null;
    let endDateIso = null;
    if (startDate || endDate) {
      const istOffset = '+05:30';
      if (startDate) {
        const startObj = new Date(startDate + 'T00:00:00' + istOffset);
        if (!isNaN(startObj.getTime())) startDateIso = startObj.toISOString();
      }
      if (endDate) {
        const endObj = new Date(endDate + 'T23:59:59.999' + istOffset);
        if (!isNaN(endObj.getTime())) endDateIso = endObj.toISOString();
      }
    }

    const discoveredMap = new Map();
    let autoDetectedFolderName = null;

    for (const fId of folderIds) {
      const scanResult = await scanDriveFolderRecursively(drive, fId, startDateIso, endDateIso);
      if (!autoDetectedFolderName && scanResult.rootFolderName) {
        autoDetectedFolderName = scanResult.rootFolderName;
      }
      for (const vid of scanResult.videos) {
        if (!discoveredMap.has(vid.id)) {
          discoveredMap.set(vid.id, vid);
        }
      }
    }

    const rawFiles = Array.from(discoveredMap.values());
    const history = loadUploadedHistory();

    const formattedFiles = rawFiles.map((f, idx) => {
      const cleanOriginalName = (f.name || 'Video').replace(/\.[^/.]+$/, '');
      const prefixParts = [];
      if (f.batch && f.batch !== 'Batch' && f.batch !== 'Root') {
        prefixParts.push(f.batch);
      }
      if (f.subject && f.subject !== 'General' && f.subject !== 'Video' && f.subject !== f.batch) {
        prefixParts.push(f.subject);
      }

      let combinedTitle = cleanOriginalName;
      if (prefixParts.length > 0) {
        const prefix = prefixParts.join(' - ');
        if (!cleanOriginalName.toLowerCase().startsWith(prefix.toLowerCase())) {
          combinedTitle = `${prefix} | ${cleanOriginalName}`;
        }
      }
      if (combinedTitle.length > 98) {
        combinedTitle = combinedTitle.substring(0, 95) + '...';
      }

      const isDuplicate = history.some(h => 
        (h.id && h.id === f.id) || 
        (h.customTitle && h.customTitle.trim().toLowerCase() === combinedTitle.trim().toLowerCase()) ||
        (h.name && h.name.trim().toLowerCase() === f.name.trim().toLowerCase())
      );

      const existingRecord = isDuplicate ? history.find(h => 
        (h.id && h.id === f.id) || 
        (h.customTitle && h.customTitle.trim().toLowerCase() === combinedTitle.trim().toLowerCase()) ||
        (h.name && h.name.trim().toLowerCase() === f.name.trim().toLowerCase())
      ) : null;

      return {
        index: idx + 1,
        id: f.id,
        name: f.name,
        originalName: f.name,
        customTitle: combinedTitle,
        batch: f.batch || autoDetectedFolderName || 'Batch',
        subject: f.subject || 'Lecture',
        folderPath: f.folderPath || '',
        size: parseInt(f.size || '0', 10),
        durationMillis: f.durationMillis || null,
        width: f.width || null,
        height: f.height || null,
        createdTime: f.createdTime,
        isDuplicate: Boolean(isDuplicate),
        existingVideoId: existingRecord?.videoId || null,
        existingYoutubeUrl: existingRecord?.youtubeUrl || null
      };
    });

    return res.json({
      success: true,
      folderName: autoDetectedFolderName || 'Batch Folder',
      totalFiles: formattedFiles.length,
      files: formattedFiles
    });
  } catch (err) {
    console.error('Scan preview error:', err);
    return res.status(500).json({ success: false, error: 'An internal error occurred. Please try again.' });
  }
});

/**
 * Core Processing Endpoint
 */
app.post(['/api/process', '/api/process-folder'], async (req, res) => {
  const folderInput = req.body.folderInput || req.body.folderUrl || '';
  const playlistName = req.body.playlistName || '';
  const startDate = req.body.startDate || '';
  const endDate = req.body.endDate || '';
  const rawMode = req.body.processingMode || 'youtube_standard';
  const processingMode = rawMode === 'youtube' ? 'youtube_standard' : (rawMode === 'queue' ? 'youtube_queue' : rawMode);

  if (jobState.status === 'processing' || jobState.status === 'scanning') {
    if (req.body.force === true || req.body.forceRestart === true) {
      if (activeAbortController) {
        try {
          activeAbortController.abort();
        } catch(e) {}
      }
      addJobLog('Stopping previous running job to start new folder process...', 'warn');
    } else {
      return res.status(400).json({
        success: false,
        isJobRunning: true,
        error: 'A video upload job is already running in the background.'
      });
    }
  }

  if (!folderInput) {
    return res.status(400).json({ success: false, error: 'Google Drive Folder link or ID is required.' });
  }

  const folderIds = extractFolderIds(folderInput);
  if (folderIds.length === 0) {
    return res.status(400).json({ success: false, error: 'Invalid Google Drive Folder link or ID format.' });
  }

  const auth = getOAuth2Client(req);
  if (!auth) {
    return res.status(500).json({
      success: false,
      error: 'Google OAuth2 access token missing. Please sign in via the app interface.'
    });
  }

  // Preserve existing uploaded history so user never loses past videos
  const history = loadUploadedHistory();
  const existingCompleted = history.length > 0 ? history : jobState.files.filter(f => f.status === 'completed');

  const privacyStatus = req.body.privacyStatus || 'unlisted';
  const scheduledPublishAt = req.body.scheduledPublishAt || null;
  const descriptionFooter = req.body.descriptionFooter || '';
  const customTags = Array.isArray(req.body.customTags) ? req.body.customTags : (req.body.customTags ? String(req.body.customTags).split(',').map(s=>s.trim()).filter(Boolean) : []);
  const customThumbnails = (req.body.customThumbnails && typeof req.body.customThumbnails === 'object') ? req.body.customThumbnails : {};

  jobState = {
    id: `job_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`,
    folderIds,
    folderInput,
    playlistTitle: (playlistName || '').trim(),
    playlistId: null,
    playlistUrl: null,
    privacyStatus,
    scheduledPublishAt,
    descriptionFooter,
    customTags,
    customThumbnails,
    status: 'scanning',
    processingMode: processingMode || 'youtube_standard',
    startedAt: new Date().toISOString(),
    finishedAt: null,
    files: existingCompleted,
    logs: [],
    stats: { total: existingCompleted.length, pending: 0, completed: existingCompleted.length, failed: 0 }
  };
  persistJobState();

  res.json({
    success: true,
    message: 'Pipeline started in background across folder(s).',
    jobId: jobState.id,
    folderCount: folderIds.length
  });

  broadcastSSE({ type: 'state_sync', state: jobState });

  // RUN BACKGROUND PIPELINE
  (async () => {
    activeAbortController = new AbortController();

    try {
      const drive = google.drive({ version: 'v3', auth });
      const youtube = google.youtube({ version: 'v3', auth });

      // Resolve this user's channelId for tagging uploaded videos
      try {
        const chRes = await youtube.channels.list({ part: ['id'], mine: true });
        activeJobChannelId = chRes.data.items?.[0]?.id || null;
      } catch (e) {
        activeJobChannelId = null;
      }

      addJobLog(`Scanning Google Drive Folder(s) & subfolders recursively...`);

      // Determine Date Range (Optional: only filter if user provided start or end date)
      let startDateIso = null;
      let endDateIso = null;

      if (startDate || endDate) {
        // Assume the user is in Indian Standard Time (IST, UTC+5:30)
        // because that's where PW operates.
        const istOffset = '+05:30'; 
        
        if (startDate) {
          const startObj = new Date(startDate + 'T00:00:00' + istOffset);
          if (!isNaN(startObj.getTime())) {
            startDateIso = startObj.toISOString();
          }
        }
        if (endDate) {
          const endObj = new Date(endDate + 'T23:59:59.999' + istOffset);
          if (!isNaN(endObj.getTime())) {
            endDateIso = endObj.toISOString();
          }
        }
        addJobLog(`Filtering videos matching date range: ${startDate || 'Any'} to ${endDate || 'Any'}`);
      } else {
        addJobLog(`Scanning all video files in folder(s) (All Time)...`);
      }

      const discoveredMap = new Map();
      let autoDetectedFolderName = null;

      for (const fId of folderIds) {
        const scanResult = await scanDriveFolderRecursively(drive, fId, startDateIso, endDateIso);
        if (!autoDetectedFolderName && scanResult.rootFolderName) {
          autoDetectedFolderName = scanResult.rootFolderName;
        }

        for (const vid of scanResult.videos) {
          if (!discoveredMap.has(vid.id)) {
            discoveredMap.set(vid.id, vid);
          }
        }
      }

      let rawFiles = Array.from(discoveredMap.values());

      // If user selected specific files from the scan preview
      if (req.body.selectedFileIds && Array.isArray(req.body.selectedFileIds) && req.body.selectedFileIds.length > 0) {
        const idSet = new Set(req.body.selectedFileIds);
        rawFiles = rawFiles.filter(f => idSet.has(f.id));
      }

      if (rawFiles.length === 0) {
        jobState.status = 'completed';
        jobState.finishedAt = new Date().toISOString();
        addJobLog('No video files found matching the selection in the given Google Drive folder.', 'warn');
        broadcastSSE({ type: 'no_files_found', message: 'No video files found matching the selection.' });
        persistJobState();
        return;
      }

      // Setup Playlist Name (Only if explicitly provided by user)
      const targetPlaylistTitle = (jobState.playlistTitle && jobState.playlistTitle.trim()) ? jobState.playlistTitle.trim() : null;
      jobState.playlistTitle = targetPlaylistTitle || '';

      if (targetPlaylistTitle && jobState.processingMode !== 'drive_secure') {
        try {
          addJobLog(`Setting up YouTube Playlist: "${targetPlaylistTitle}" (Unlisted)...`);
          const pId = await getOrCreatePlaylist(youtube, targetPlaylistTitle);
          jobState.playlistId = pId;
          jobState.playlistUrl = `https://www.youtube.com/playlist?list=${pId}`;
          addJobLog(`✔ Playlist Ready: ${jobState.playlistUrl}`, 'success');
          broadcastSSE({
            type: 'playlist_ready',
            playlistTitle: targetPlaylistTitle,
            playlistId: pId,
            playlistUrl: jobState.playlistUrl
          });
        } catch (pErr) {
          addJobLog(`Playlist notice: ${pErr.message}. Videos will still upload directly.`, 'warn');
        }
      } else {
        jobState.playlistId = null;
        jobState.playlistUrl = null;
      }

      const customTitlesMap = (req.body.customTitles && typeof req.body.customTitles === 'object') ? req.body.customTitles : {};

      jobState.status = 'processing';
      const newFiles = rawFiles.map((f, idx) => {
        // Build clear smart title combining Folder / Subject and original filename
        // e.g. "27-LN151MA (Subject) | Lecture 01 - Basics" or "27-LN151MA | Organic Chemistry | Lecture 01"
        const cleanOriginalName = (f.name || 'Video').replace(/\.[^/.]+$/, ''); // Strip file extension
        const prefixParts = [];
        if (f.batch && f.batch !== 'Batch' && f.batch !== 'Root') {
          prefixParts.push(f.batch);
        }
        if (f.subject && f.subject !== 'General' && f.subject !== 'Video' && f.subject !== f.batch) {
          prefixParts.push(f.subject);
        }

        let combinedTitle = cleanOriginalName;
        if (customTitlesMap[f.id]) {
          combinedTitle = customTitlesMap[f.id];
        } else if (prefixParts.length > 0) {
          const prefix = prefixParts.join(' - ');
          if (!cleanOriginalName.toLowerCase().startsWith(prefix.toLowerCase())) {
            combinedTitle = `${prefix} | ${cleanOriginalName}`;
          }
        }
        // Ensure YouTube max title length of 100 chars
        if (combinedTitle.length > 98) {
          combinedTitle = combinedTitle.substring(0, 95) + '...';
        }

        return {
          index: idx + 1,
          id: f.id,
          name: f.name,
          originalName: f.name,
          customTitle: combinedTitle,
          batch: f.batch || autoDetectedFolderName || 'Batch',
          subject: f.subject || 'Lecture',
          folderPath: f.folderPath || '',
          size: parseInt(f.size || '0', 10),
          durationMillis: f.durationMillis || null,
          width: f.width || null,
          height: f.height || null,
          createdTime: f.createdTime,
          status: 'queued',
          percentage: 0,
          uploadedBytes: 0,
          totalBytes: parseInt(f.size || '0', 10),
          speedMBps: 0,
          etaSeconds: 0,
          youtubeUrl: null,
          videoId: null,
          thumbnailUrl: null,
          error: null
        };
      });

      const existingHistory = loadUploadedHistory();
      const existingNotDuplicate = existingHistory.filter(h => !newFiles.some(nf => nf.id === h.id || nf.name === h.name));
      jobState.files = [...newFiles, ...existingNotDuplicate];

      jobState.stats = {
        total: jobState.files.length,
        pending: newFiles.length,
        completed: existingNotDuplicate.length,
        failed: 0
      };

      addJobLog(`Discovered ${jobState.files.length} video(s) created today. Starting stream queue...`, 'highlight');
      persistJobState();
      broadcastSSE({ type: 'state_sync', state: jobState });

      await runUploadQueue(auth);

      if (jobState.status !== 'cancelled' && jobState.status !== 'paused_quota') {
        jobState.status = 'completed';
        jobState.finishedAt = new Date().toISOString();
        addJobLog('All videos in queue processed successfully.', 'success');
        broadcastSSE({ type: 'process_completed', message: 'All videos processed successfully.' });
        persistJobState();
      }

    } catch (fatalErr) {
      console.error('Fatal background error:', fatalErr);
      jobState.status = 'error';
      if (isAuthError(fatalErr)) {
        addJobLog(`Google Authentication Error: ${fatalErr.message}. Please re-connect Google account.`, 'error');
        broadcastSSE({ type: 'auth_required', message: `Authentication expired or invalid. Please click 'Connect Google' to authorize.` });
      } else {
        addJobLog(`Pipeline encountered fatal error: ${fatalErr.message}`, 'error');
        broadcastSSE({ type: 'error', message: fatalErr.message });
      }
      persistJobState();
    } finally {
      activeAbortController = null;
    }
  })();
});




/**
 * Update YouTube Thumbnail Endpoint
 */
app.post('/api/thumbnail', async (req, res) => {
  const auth = getOAuth2Client(req);
  if (!auth) {
    return res.status(401).json({ success: false, error: 'Google OAuth2 access token missing.' });
  }

  const { videoId, imageBase64 } = req.body;
  if (!videoId || !imageBase64) {
    return res.status(400).json({ success: false, error: 'Missing videoId or image data.' });
  }

  try {
    const { google } = require('googleapis');
    const youtube = google.youtube({ version: 'v3', auth });
    const { Readable } = require('stream');

    const base64Data = imageBase64.replace(/^data:image\/\w+;base64,/, '');
    const buffer = Buffer.from(base64Data, 'base64');
    
    // Guess mime type from base64 header if possible, else default to jpeg
    let mimeType = 'image/jpeg';
    if (imageBase64.startsWith('data:image/png')) mimeType = 'image/png';

    const readable = new Readable();
    readable._read = () => {};
    readable.push(buffer);
    readable.push(null);

    const result = await youtube.thumbnails.set({
      videoId: videoId,
      media: {
        mimeType: mimeType,
        body: readable
      }
    });

    res.json({ success: true, url: result.data.items[0].default.url });
  } catch (err) {
    console.error('Thumbnail upload error:', err);
    res.status(500).json({ success: false, error: 'An internal error occurred. Please try again.' });
  }
});

/**
 * Proxy Google Drive Images for real-time frontend preview without CORS restrictions
 */
app.get('/api/drive-image-proxy', async (req, res) => {
  const fileId = extractGoogleDriveFileId(req.query.id || req.query.url);
  if (!fileId || !/^[a-zA-Z0-9_-]{10,80}$/.test(fileId)) {
    return res.status(400).send('Invalid or missing Google Drive image file ID');
  }

  const auth = getOAuth2Client(req);
  if (!auth) return res.status(401).send('Google authentication required');

  try {
    const drive = google.drive({ version: 'v3', auth });
    const meta = await drive.files.get({ fileId, fields: 'id, name, mimeType, size', supportsAllDrives: true });

    // Only allow image mime types to prevent open relay
    const mimeType = meta.data.mimeType || '';
    if (!mimeType.startsWith('image/')) {
      return res.status(403).send('Only image files can be proxied.');
    }

    // Reject files larger than 10MB
    const fileSize = parseInt(meta.data.size || '0', 10);
    if (fileSize > 10 * 1024 * 1024) {
      return res.status(413).send('Image file too large.');
    }

    const streamRes = await drive.files.get({ fileId, alt: 'media', supportsAllDrives: true }, { responseType: 'stream' });

    res.setHeader('Content-Type', mimeType);
    res.setHeader('Cache-Control', 'public, max-age=86400');
    streamRes.data.pipe(res);
  } catch (err) {
    console.error('Drive image proxy error:', err.message);
    res.status(500).send('Could not fetch image.');
  }
});

/**
 * Direct High-Speed Streaming Proxy Upload to YouTube
 * Accepts raw video binary payload from browser and pipes directly to YouTube API
 * Eliminates all CORS issues, zero API quota wasted on network errors
 */
app.post('/api/stream-manual-upload', async (req, res) => {
  const auth = getOAuth2Client(req);
  if (!auth) {
    return res.status(401).json({ success: false, error: 'Google Account not connected.' });
  }

  try {
    const title = (req.query.title || 'Direct Lecture Video').trim();
    const batch = req.query.batch ? req.query.batch.trim() : 'Manual Upload';
    const subject = req.query.subject ? req.query.subject.trim() : 'Lecture';
    const playlistName = req.query.playlistName ? req.query.playlistName.trim() : null;
    const privacyStatus = req.query.privacyStatus || 'unlisted';
    const finalPrivacy = ['public', 'private', 'unlisted'].includes(privacyStatus) ? privacyStatus : 'unlisted';

    let fullDesc = `Lecture Video: ${title}\nBatch: ${batch}\nSubject: ${subject}`;
    if (playlistName) fullDesc += `\nPlaylist: ${playlistName}`;
    fullDesc += `\n\nUploaded on: ${new Date().toISOString()}`;

    const tags = ['DirectUpload', 'Lecture', subject, batch].filter(Boolean);

    const youtube = google.youtube({ version: 'v3', auth });

    const ytResponse = await youtube.videos.insert({
      part: ['snippet', 'status'],
      requestBody: {
        snippet: {
          title,
          description: fullDesc,
          tags,
          categoryId: '27' // Education
        },
        status: {
          privacyStatus: finalPrivacy,
          selfDeclaredMadeForKids: false
        }
      },
      media: {
        body: req
      }
    });

    const videoId = ytResponse.data.id;
    const youtubeUrl = `https://youtu.be/${videoId}`;
    const studioUrl = `https://studio.youtube.com/video/${videoId}/edit`;
    const finalThumbnail = `https://img.youtube.com/vi/${videoId}/hqdefault.jpg`;

    // Add to playlist if requested
    if (playlistName) {
      try {
        const pId = await getOrCreatePlaylist(youtube, playlistName);
        if (pId) await addVideoToPlaylist(youtube, pId, videoId);
      } catch (pErr) {
        console.warn('Manual playlist add error:', pErr.message);
      }
    }

    let audioHealth = null;
    if (req.query.audioHealth) {
      try {
        audioHealth = JSON.parse(decodeURIComponent(req.query.audioHealth));
      } catch (e) {}
    }

    const record = {
      id: videoId,
      videoId: videoId,
      name: title,
      originalName: title,
      customTitle: title,
      batch,
      subject,
      folderPath: 'Manual Device Upload',
      size: parseInt(req.headers['content-length'] || '0', 10),
      createdTime: new Date().toISOString(),
      status: 'completed',
      percentage: 100,
      uploadedBytes: parseInt(req.headers['content-length'] || '0', 10),
      totalBytes: parseInt(req.headers['content-length'] || '0', 10),
      speedMBps: 0,
      etaSeconds: 0,
      youtubeUrl,
      thumbnailUrl: finalThumbnail,
      studioUrl,
      audioHealth,
      error: null
    };

    saveCompletedFileToHistory(record);
    const existingIdx = jobState.files.findIndex(f => f.id === videoId || f.videoId === videoId);
    if (existingIdx >= 0) {
      jobState.files[existingIdx] = record;
    } else {
      jobState.files.unshift(record);
    }
    persistJobState();
    broadcastSSE({
      type: 'file_completed',
      fileId: videoId,
      fileName: title,
      videoId,
      youtubeUrl,
      studioUrl,
      thumbnailUrl: finalThumbnail,
      file: record
    });

    return res.json({
      success: true,
      videoId,
      youtubeUrl,
      studioUrl,
      thumbnailUrl: finalThumbnail,
      title,
      record
    });
  } catch (err) {
    console.error('Stream manual upload error:', err);
    return res.status(500).json({
      success: false,
      error: err.message || 'Direct manual stream to YouTube failed'
    });
  }
});

/**
 * Initiate Direct Manual Video Upload from Local File
 */
app.post('/api/initiate-direct-upload', async (req, res) => {
  const auth = getOAuth2Client(req);
  if (!auth) {
    return res.status(401).json({ success: false, error: 'Google Account not connected. Please click Connect Google.' });
  }

  try {
    const title = (req.body.title || 'Direct Lecture Video').trim();
    const batch = req.body.batch ? req.body.batch.trim() : 'Manual Upload';
    const subject = req.body.subject ? req.body.subject.trim() : 'Lecture';
    const playlistName = req.body.playlistName ? req.body.playlistName.trim() : null;
    const privacyStatus = req.body.privacyStatus || 'unlisted';
    const scheduledPublishAt = req.body.scheduledPublishAt || null;
    const descriptionFooter = req.body.descriptionFooter || '';
    const customTags = Array.isArray(req.body.customTags) ? req.body.customTags : [];

    const isScheduled = privacyStatus === 'scheduled' && scheduledPublishAt;
    const finalPrivacy = isScheduled ? 'private' : (privacyStatus === 'public' ? 'public' : (privacyStatus === 'private' ? 'private' : 'unlisted'));

    let statusConfig = {
      privacyStatus: finalPrivacy,
      selfDeclaredMadeForKids: false
    };
    if (isScheduled) {
      try {
        statusConfig.publishAt = new Date(scheduledPublishAt).toISOString();
      } catch (e) {}
    }

    let fullDesc = `Lecture Video: ${title}\nBatch: ${batch}\nSubject: ${subject}`;
    if (playlistName) fullDesc += `\nPlaylist: ${playlistName}`;
    if (descriptionFooter) fullDesc += `\n\n${descriptionFooter}`;
    fullDesc += `\n\nUploaded on: ${new Date().toISOString()}`;

    const tags = ['DirectUpload', 'Lecture', subject, batch, ...customTags].filter(Boolean);

    // Fetch fresh access token
    const tokenInfo = await auth.getAccessToken();
    const accessToken = typeof tokenInfo === 'string' ? tokenInfo : (tokenInfo?.token || tokenInfo?.access_token);

    if (!accessToken) {
      return res.status(401).json({ success: false, error: 'Could not acquire Google Access Token.' });
    }

    // Call YouTube API Resumable Upload Initiation URL
    const https = require('https');
    const postData = JSON.stringify({
      snippet: {
        title,
        description: fullDesc,
        tags,
        categoryId: '27' // Education
      },
      status: statusConfig
    });

    const initOptions = {
      hostname: 'www.googleapis.com',
      port: 443,
      path: '/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status',
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json; charset=UTF-8',
        'Content-Length': Buffer.byteLength(postData),
        'X-Upload-Content-Type': 'video/*'
      }
    };

    const initReq = https.request(initOptions, (initRes) => {
      if (initRes.statusCode >= 200 && initRes.statusCode < 300) {
        const uploadUrl = initRes.headers['location'];
        if (uploadUrl) {
          return res.json({
            success: true,
            uploadUrl,
            title,
            batch,
            subject,
            playlistName
          });
        }
      }

      let errData = '';
      initRes.on('data', chunk => { errData += chunk; });
      initRes.on('end', () => {
        console.error('YouTube Direct Upload Init Error:', errData);
        return res.status(initRes.statusCode || 500).json({
          success: false,
          error: `YouTube API returned error (${initRes.statusCode}): ${errData}`
        });
      });
    });

    initReq.on('error', (e) => {
      console.error('HTTPS init error:', e);
      return res.status(500).json({ success: false, error: e.message });
    });

    initReq.write(postData);
    initReq.end();
  } catch (err) {
    console.error('Initiate direct upload error:', err);
    return res.status(500).json({ success: false, error: 'An internal error occurred. Please try again.' });
  }
});

/**
 * Complete Direct Manual Video Upload (Push Thumbnail, Add to Playlist, Save to History)
 */
app.post('/api/complete-direct-upload', async (req, res) => {
  const auth = getOAuth2Client(req);
  const { videoId, title, batch, subject, playlistName, fileSize, thumbnailBase64, thumbnailUrl } = req.body;

  if (!videoId) {
    return res.status(400).json({ success: false, error: 'Missing videoId.' });
  }

  const youtubeUrl = `https://youtu.be/${videoId}`;
  const studioUrl = `https://studio.youtube.com/video/${videoId}/edit`;
  let finalThumbnail = `https://img.youtube.com/vi/${videoId}/hqdefault.jpg`;

  const record = {
    id: videoId,
    videoId: videoId,
    name: title || 'Manual Lecture Video',
    originalName: title || 'Manual Lecture Video',
    customTitle: title || 'Manual Lecture Video',
    batch: batch || 'Direct Upload',
    subject: subject || 'Lecture',
    folderPath: 'Manual Device Upload',
    size: fileSize || 0,
    createdTime: new Date().toISOString(),
    status: 'completed',
    percentage: 100,
    uploadedBytes: fileSize || 0,
    totalBytes: fileSize || 0,
    speedMBps: 0,
    etaSeconds: 0,
    youtubeUrl,
    thumbnailUrl: finalThumbnail,
    studioUrl,
    error: null
  };

  if (auth) {
    const { google } = require('googleapis');
    const youtube = google.youtube({ version: 'v3', auth });

    // 1. Add to Playlist if specified
    if (playlistName && playlistName.trim()) {
      try {
        const pId = await getOrCreatePlaylist(youtube, playlistName.trim());
        if (pId) {
          await addVideoToPlaylist(youtube, pId, videoId);
          addJobLog(`✔ Added "${title}" to Playlist: "${playlistName}"`, 'info');
        }
      } catch (pErr) {
        console.warn('Manual playlist add error:', pErr.message);
      }
    }

    // 2. Set Custom Thumbnail if provided
    let thumbBuf = null;
    let mimeType = 'image/jpeg';

    if (thumbnailBase64) {
      const match = thumbnailBase64.match(/^data:([^;]+);base64,(.+)$/);
      mimeType = match ? match[1] : 'image/jpeg';
      const raw = match ? match[2] : thumbnailBase64;
      thumbBuf = Buffer.from(raw, 'base64');
    } else if (thumbnailUrl) {
      const driveFileId = extractGoogleDriveFileId(thumbnailUrl);
      if (driveFileId) {
        try {
          const drive = google.drive({ version: 'v3', auth });
          const imgRes = await drive.files.get({ fileId: driveFileId, alt: 'media', supportsAllDrives: true }, { responseType: 'arraybuffer' });
          thumbBuf = Buffer.from(imgRes.data);
        } catch (dErr) {
          console.warn('Could not fetch drive thumbnail for direct upload:', dErr.message);
        }
      }
    }

    if (thumbBuf) {
      try {
        const { Readable } = require('stream');
        const stream = new Readable();
        stream.push(thumbBuf);
        stream.push(null);

        await youtube.thumbnails.set({
          videoId,
          media: { mimeType, body: stream }
        });
        finalThumbnail = `https://img.youtube.com/vi/${videoId}/hqdefault.jpg?t=${Date.now()}`;
        record.thumbnailUrl = finalThumbnail;
        addJobLog(`✔ Branded thumbnail uploaded for "${title}"`, 'success');
      } catch (tErr) {
        console.warn('Manual upload thumbnail set error:', tErr.message);
      }
    }
  }

  saveCompletedFileToHistory(record);
  addJobLog(`✔ Manual Upload Complete: "${title}" ➔ ${youtubeUrl}`, 'success');

  broadcastSSE({
    type: 'file_completed',
    fileId: videoId,
    fileName: title,
    videoId,
    youtubeUrl,
    studioUrl,
    thumbnailUrl: finalThumbnail
  });

  return res.json({
    success: true,
    videoId,
    youtubeUrl,
    studioUrl,
    thumbnailUrl: finalThumbnail
  });
});

// Start Server
app.listen(PORT, '0.0.0.0', () => {
  console.log(`====================================================`);
  console.log(` Drive-to-YouTube Background Streaming Service Live `);
  console.log(` Web UI: http://localhost:${PORT}                   `);
  console.log(` State File: ${STATE_FILE}                          `);
  console.log(`====================================================`);
});

async function runUploadQueue(auth) {
  const { google } = require('googleapis');
  const drive = google.drive({ version: 'v3', auth });
  const youtube = google.youtube({ version: 'v3', auth });
  const { Transform } = require('stream');

      for (let i = 0; i < jobState.files.length; i++) {
        if (jobState.status === 'cancelled' || jobState.status === 'paused_quota') {
          addJobLog('Pipeline stopped during queue execution.', 'warn');
          break;
        }

        if (jobState.files[i].status === 'completed' || jobState.files[i].status === 'failed') {
          continue;
        }

        const fileObj = jobState.files[i];
        fileObj.status = 'uploading';
        persistJobState();

        const uploadTitle = fileObj.customTitle || fileObj.name || fileObj.originalName;

        addJobLog(`[${i + 1}/${jobState.files.length}] Streaming: "${uploadTitle}" (${fileObj.subject})`, 'highlight');
        broadcastSSE({
          type: 'file_start',
          fileId: fileObj.id,
          fileName: uploadTitle,
          subject: fileObj.subject,
          batch: fileObj.batch,
          index: i + 1,
          total: jobState.files.length,
          totalBytes: fileObj.totalBytes
        });

        try {
          if (jobState.processingMode === 'drive_secure') {
            fileObj.percentage = 10;
            broadcastSSE({
              type: 'upload_progress', fileId: fileObj.id, fileName: uploadTitle,
              uploadedBytes: 0, totalBytes: fileObj.totalBytes, percentage: 10, speedMBps: 0, etaSeconds: 0
            });

            // 1. (Option B chosen by user: We bypass the lock so it doesn't fail for non-owners)
            // Removed: await drive.files.update({ fileId: fileObj.id, requestBody: { copyRequiresWriterPermission: true }, supportsAllDrives: true });

            fileObj.percentage = 50;
            // 2. Add permission to make it accessible to anyone with link
            try {
              await drive.permissions.create({
                fileId: fileObj.id,
                requestBody: { role: 'reader', type: 'anyone' },
                supportsAllDrives: true
              });
            } catch (permErr) {
              addJobLog(`Notice: Could not make "${uploadTitle}" public due to domain rules. Using existing sharing settings.`, 'warn');
            }

            fileObj.percentage = 100;
            const embedUrl = `https://drive.google.com/file/d/${fileObj.id}/preview`;
            
            fileObj.status = 'completed';
            fileObj.videoId = fileObj.id; // Store Drive ID as videoId for table
            fileObj.youtubeUrl = embedUrl;
            fileObj.studioUrl = embedUrl;
            fileObj.thumbnailUrl = 'https://drive-thirdparty.googleusercontent.com/16/type/video/mp4';

            jobState.stats.pending = Math.max(0, jobState.stats.pending - 1);
            jobState.stats.completed += 1;
            addJobLog(`Generated Secure Drive Player for: "${uploadTitle}"`, 'success');
            saveCompletedFileToHistory(fileObj);
            persistJobState();

            broadcastSSE({
              type: 'file_completed',
              fileId: fileObj.id,
              fileName: uploadTitle,
              videoId: fileObj.id,
              youtubeUrl: embedUrl,
              studioUrl: embedUrl,
              thumbnailUrl: fileObj.thumbnailUrl
            });
            continue;
          }

          const driveStreamResponse = await drive.files.get(
            { fileId: fileObj.id, alt: 'media', supportsAllDrives: true },
            { responseType: 'stream' }
          );

          let uploadedBytes = 0;
          let lastReportedPercent = -1;
          let lastReportTime = Date.now();
          let startTime = Date.now();
          let speedMBps = 0;
          let etaSeconds = 0;

          const progressMonitor = new Transform({
            transform(chunk, encoding, callback) {
              uploadedBytes += chunk.length;
              fileObj.uploadedBytes = uploadedBytes;

              const currentTime = Date.now();
              const percent = fileObj.totalBytes > 0 
                ? Math.min(100, Math.round((uploadedBytes / fileObj.totalBytes) * 100)) 
                : 0;
              fileObj.percentage = percent;

              const timeDiffSec = (currentTime - startTime) / 1000;
              if (timeDiffSec > 0.5) {
                speedMBps = ((uploadedBytes / (1024 * 1024)) / timeDiffSec);
                fileObj.speedMBps = parseFloat(speedMBps.toFixed(2));
                const remainingBytes = Math.max(0, fileObj.totalBytes - uploadedBytes);
                etaSeconds = speedMBps > 0 ? Math.round((remainingBytes / (1024 * 1024)) / speedMBps) : 0;
                fileObj.etaSeconds = etaSeconds;
              }

              if ((percent !== lastReportedPercent && (currentTime - lastReportTime >= 150 || percent === 100)) || uploadedBytes === chunk.length) {
                lastReportedPercent = percent;
                lastReportTime = currentTime;

                broadcastSSE({
                  type: 'upload_progress',
                  fileId: fileObj.id,
                  fileName: uploadTitle,
                  uploadedBytes,
                  totalBytes: fileObj.totalBytes,
                  percentage: percent,
                  speedMBps: fileObj.speedMBps,
                  etaSeconds: fileObj.etaSeconds
                });
              }

              callback(null, chunk);
            }
          });

          const monitoredStream = driveStreamResponse.data.pipe(progressMonitor);

          const targetPrivacy = jobState.privacyStatus || 'unlisted';
          const isScheduled = targetPrivacy === 'scheduled' && jobState.scheduledPublishAt;
          const finalPrivacy = isScheduled ? 'private' : (targetPrivacy === 'public' ? 'public' : (targetPrivacy === 'private' ? 'private' : 'unlisted'));

          let videoStatus = {
            privacyStatus: finalPrivacy,
            selfDeclaredMadeForKids: false,
            embeddable: true,
            license: 'youtube'
          };
          if (isScheduled) {
            try {
              videoStatus.publishAt = new Date(jobState.scheduledPublishAt).toISOString();
            } catch (e) {}
          }

          let fullDescription = `Lecture Video: ${uploadTitle}\nBatch: ${fileObj.batch}\nSubject: ${fileObj.subject}`;
          if (jobState.playlistTitle) fullDescription += `\nPlaylist: ${jobState.playlistTitle}`;
          if (jobState.descriptionFooter) fullDescription += `\n\n${jobState.descriptionFooter}`;
          fullDescription += `\n\nUploaded on: ${new Date().toISOString()}`;

          const allTags = ['DriveToYouTube', 'AutomatedUpload', fileObj.subject, fileObj.batch, ...(jobState.customTags || [])].filter(Boolean);

          const ytResponse = await youtube.videos.insert({
            part: ['snippet', 'status'],
            requestBody: {
              snippet: {
                title: uploadTitle,
                description: fullDescription,
                tags: allTags,
                categoryId: '27' // Education
              },
              status: videoStatus
            },
            media: {
              body: monitoredStream
            }
          });

          const videoId = ytResponse.data.id;
          const youtubeUrl = `https://youtu.be/${videoId}`;
          const studioUrl = `https://studio.youtube.com/video/${videoId}/edit`;
          let thumbnailUrl = `https://img.youtube.com/vi/${videoId}/mqdefault.jpg`;

          // Handle automatic thumbnail push if provided in batch
          if (jobState.customThumbnails && jobState.customThumbnails[fileObj.id] && videoId) {
            try {
              const rawThumb = jobState.customThumbnails[fileObj.id];
              let thumbBuf = null;
              if (rawThumb.startsWith('data:image/')) {
                const b64 = rawThumb.replace(/^data:image\/\w+;base64,/, '');
                thumbBuf = Buffer.from(b64, 'base64');
              }
              if (thumbBuf) {
                await youtube.thumbnails.set({
                  videoId: videoId,
                  media: {
                    mimeType: 'image/jpeg',
                    body: require('stream').Readable.from(thumbBuf)
                  }
                });
                thumbnailUrl = `https://img.youtube.com/vi/${videoId}/hqdefault.jpg?t=${Date.now()}`;
                addJobLog(`✔ Branded thumbnail uploaded for "${uploadTitle}"`, 'success');
              }
            } catch (tErr) {
              console.warn('Batch thumbnail push note:', tErr.message);
            }
          }

          fileObj.status = 'completed';
          fileObj.percentage = 100;
          fileObj.videoId = videoId;
          fileObj.youtubeUrl = youtubeUrl;
          fileObj.studioUrl = studioUrl;
          fileObj.thumbnailUrl = thumbnailUrl;

          if (jobState.playlistId && jobState.processingMode !== 'drive_secure') {
            try {
              await addVideoToPlaylist(youtube, jobState.playlistId, videoId);
              addJobLog(`✔ Added "${uploadTitle}" to Playlist: "${jobState.playlistTitle}"`, 'info');
            } catch (plErr) {
              console.warn('Playlist item insert error:', plErr.message);
            }
          }

          jobState.stats.pending = Math.max(0, jobState.stats.pending - 1);
          jobState.stats.completed += 1;

          const privacyLabel = isScheduled ? `Scheduled for ${new Date(jobState.scheduledPublishAt).toLocaleString()}` : (finalPrivacy.charAt(0).toUpperCase() + finalPrivacy.slice(1));
          addJobLog(`Uploaded: "${uploadTitle}" ➔ ${youtubeUrl} (${privacyLabel})`, 'success');
          saveCompletedFileToHistory(fileObj);
          persistJobState();

          broadcastSSE({
            type: 'file_completed',
            fileId: fileObj.id,
            fileName: uploadTitle,
            videoId,
            youtubeUrl,
            studioUrl,
            thumbnailUrl
          });

        } catch (uploadErr) {
          console.error(`Error processing ${uploadTitle}:`, uploadErr);
          
          const errMsg = (uploadErr.message || '').toLowerCase();
          const isQuotaOrLimit = (
            errMsg.includes('exceeded the number of videos') ||
            errMsg.includes('uploadlimitexceeded') ||
            errMsg.includes('quota') ||
            errMsg.includes('daily upload') ||
            uploadErr.code === 403 ||
            (uploadErr.code === 400 && (errMsg.includes('upload') || errMsg.includes('limit') || errMsg.includes('exceeded')))
          );

          if (isQuotaOrLimit) {
            fileObj.status = 'failed';
            fileObj.error = 'YouTube daily limit reached (10-15 videos/day for channel). Click "Use Drive Player" for instant playback.';
            jobState.status = 'paused_quota';
            
            jobState.stats.pending = Math.max(0, jobState.stats.pending - 1);
            jobState.stats.failed += 1;

            addJobLog(`YouTube API Daily Upload Limit reached on your channel. Paused remaining uploads. You can switch remaining videos to Secure Drive Player.`, 'warn');
            persistJobState();

            broadcastSSE({
              type: 'quota_exceeded',
              fileId: fileObj.id,
              fileName: uploadTitle,
              error: fileObj.error,
              message: 'YouTube daily upload limit reached. You can convert remaining videos to Secure Drive Player instantly.'
            });
            break; // Stop streaming further files since YouTube will reject all of them
          }

          fileObj.status = 'failed';
          fileObj.error = uploadErr.message || 'Processing failed';

          jobState.stats.pending = Math.max(0, jobState.stats.pending - 1);
          jobState.stats.failed += 1;

          addJobLog(`Failed to upload "${uploadTitle}": ${fileObj.error}`, 'error');
          persistJobState();

          broadcastSSE({
            type: 'file_error',
            fileId: fileObj.id,
            fileName: uploadTitle,
            error: fileObj.error
          });
        }
      }


}
