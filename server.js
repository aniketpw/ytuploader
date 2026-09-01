/**
 * Drive to YouTube Unlisted Video Streaming Pipeline
 * Real-time SSE tracking, Recursive Subfolder Scanning, Auto-Playlist Generation, Batch/Subject Detection, and Live Preview
 */

require('dotenv').config();

process.on('uncaughtException', (err) => {
  console.error('[UNCAUGHT EXCEPTION RECOVERED]', err);
});
process.on('unhandledRejection', (reason, promise) => {
  console.error('[UNHANDLED REJECTION RECOVERED]', reason);
});

const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const { google } = require('googleapis');
const { Transform } = require('stream');
const db = require('./db');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');

const app = express();
const PORT = process.env.PORT || 3000;
const DATA_DIR = path.join(__dirname, 'data');

function loadUploadedHistory() { return db.loadUploadedHistory(); }
function persistUploadedHistory(history) { db.persistUploadedHistory(history); }
function saveCompletedFileToHistory(fileObj) { db.saveCompletedFileToHistory(fileObj); }


// Middleware
app.use(helmet({
  contentSecurityPolicy: false,
  crossOriginEmbedderPolicy: false
}));

// ══════════════════════════════════════════════════════════════════
// SILENT VISITOR TRACKER (Google Sheets Webhook)
// Placed at top so GET / and all incoming visitors are logged
// ══════════════════════════════════════════════════════════════════
const GOOGLE_SHEET_WEBHOOK = process.env.VISITOR_LOG_WEBHOOK || 'https://script.google.com/macros/s/AKfycbxPzp5iv_ukhgiR_1ZydNfg7Th7WmnIBJda00aaz4meXB_fYHSJ_Riu3AzTYLGgIq_yGg/exec';
const visitorCooldownMap = new Map();

function parseUserAgent(ua = '') {
  if (!ua) return 'Unknown Device';
  let os = 'Unknown OS';
  if (/android/i.test(ua)) os = 'Android';
  else if (/iphone|ipad|ipod/i.test(ua)) os = 'iOS';
  else if (/windows/i.test(ua)) os = 'Windows';
  else if (/macintosh|mac os/i.test(ua)) os = 'macOS';
  else if (/linux/i.test(ua)) os = 'Linux';

  let browser = 'Browser';
  if (/edg/i.test(ua)) browser = 'Edge';
  else if (/chrome|crios/i.test(ua)) browser = 'Chrome';
  else if (/safari/i.test(ua) && !/chrome|crios/i.test(ua)) browser = 'Safari';
  else if (/firefox|fxios/i.test(ua)) browser = 'Firefox';
  else if (/opera|opr/i.test(ua)) browser = 'Opera';

  const isMobile = /mobile|android|iphone/i.test(ua);
  return `${os} (${browser}${isMobile ? ' Mobile' : ''})`;
}

function getClientIp(req) {
  const xForwarded = req.headers['x-forwarded-for'];
  if (xForwarded && typeof xForwarded === 'string') {
    return xForwarded.split(',')[0].trim();
  }
  return req.headers['cf-connecting-ip'] || 
         req.headers['x-real-ip'] || 
         req.headers['true-client-ip'] || 
         req.socket?.remoteAddress || 
         req.ip || '';
}

async function silentLogVisitor(req) {
  try {
    if (!GOOGLE_SHEET_WEBHOOK) return;

    const pathName = req.path || '';
    // Skip internal health checks and asset files
    if (pathName.startsWith('/health') || pathName.startsWith('/api/health') || pathName === '/events') return;
    const ext = path.extname(pathName);
    if (ext && ext !== '.html') return;

    const rawIp = getClientIp(req);
    const cleanIp = rawIp.replace(/^::ffff:/, '').trim();
    if (!cleanIp) return;

    // Fast 30s cooldown per IP so test refreshes still register
    const now = Date.now();
    const lastLogged = visitorCooldownMap.get(cleanIp);
    if (lastLogged && (now - lastLogged) < 30 * 1000) {
      return;
    }
    visitorCooldownMap.set(cleanIp, now);

    const userAgent = req.headers['user-agent'] || '';
    const deviceStr = parseUserAgent(userAgent);
    const authHeader = req.headers.authorization || '';
    const token = authHeader.replace(/^Bearer\s+/i, '').trim();
    const cachedUser = token ? (tokenChannelCache?.get(token) || 'Authenticated User') : 'Guest';

    let city = 'Detecting...';
    let region = '';
    let country = '';
    let isp = '';

    const isLocal = cleanIp === '127.0.0.1' || cleanIp === '::1' || cleanIp === 'localhost' || cleanIp.startsWith('192.168.') || cleanIp.startsWith('10.');
    if (!isLocal) {
      try {
        const geoRes = await fetch(`http://ip-api.com/json/${cleanIp}?fields=status,country,regionName,city,isp,org`, {
          signal: AbortSignal.timeout(3000)
        });
        if (geoRes.ok) {
          const geoData = await geoRes.json();
          if (geoData.status === 'success') {
            city = geoData.city || '';
            region = geoData.regionName || '';
            country = geoData.country || '';
            isp = geoData.isp || geoData.org || '';
          }
        }
      } catch (_) {
        city = 'Online Visitor';
      }
    } else {
      city = 'Localhost / Internal';
      country = 'Local';
    }

    const timestamp = new Date().toLocaleString('en-IN', {
      timeZone: 'Asia/Kolkata',
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: true
    });

    const payload = {
      timestamp,
      ip: cleanIp,
      city: city || 'Online Visitor',
      region: region || '',
      country: country || 'India',
      isp: isp || 'Mobile/Broadband Network',
      device: deviceStr,
      email: cachedUser,
      page: pathName || '/'
    };

    console.log(`[VISITOR TELEMETRY] Logging hit from ${cleanIp} (${deviceStr})`);

    fetch(GOOGLE_SHEET_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    }).then(r => r.text()).then(res => {
      console.log(`[VISITOR LOGGED] Success: ${cleanIp} -> Google Sheets`);
    }).catch(err => {
      console.error('[VISITOR LOG ERROR]', err.message);
    });
  } catch (err) {
    console.error('[VISITOR LOG EXCEPTION]', err.message);
  }
}

// Global visitor tracker middleware at top (only for main page visits)
app.use((req, res, next) => {
  if (req.method === 'GET' && (req.path === '/' || req.path === '/index.html')) {
    setImmediate(() => silentLogVisitor(req));
  }
  next();
});

// Health Check & Telemetry Endpoints
app.get(['/health', '/api/health', '/ping', '/up'], (req, res) => {
  res.status(200).json({ status: 'ok', uptime: process.uptime(), timestamp: new Date().toISOString() });
});
app.post(['/api/ping', '/api/telemetry'], (req, res) => {
  res.status(200).json({ ok: 1 });
});

app.use(cors({
  origin: true,
  credentials: true
}));
app.use('/api/', rateLimit({
  windowMs: 60 * 1000,
  max: 5000, // Scaled for 50-60+ concurrent active users
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
app.use(express.static(path.join(__dirname, 'public'), {
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.html')) {
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
      res.setHeader('Pragma', 'no-cache');
      res.setHeader('Expires', '0');
    }
  }
}));

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

function sanitizeYouTubeTitle(title) {
  if (!title || typeof title !== 'string') return 'Lecture Video';
  let cleaned = title.replace(/[<>]/g, '').trim();
  if (!cleaned) cleaned = 'Lecture Video';
  if (cleaned.length > 100) {
    cleaned = cleaned.substring(0, 100).trim();
  }
  return cleaned;
}

function filterHistoryByChannel(history, channelId) {
  if (!channelId) return [];
  return history.filter(h => h.channelId === channelId);
}

function fetchUrlAsBuffer(url) {
  return new Promise((resolve, reject) => {
    const proto = url.startsWith('https') ? require('https') : require('http');
    proto.get(url, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetchUrlAsBuffer(res.headers.location).then(resolve, reject);
      }
      if (res.statusCode !== 200) {
        return reject(new Error(`HTTP ${res.statusCode}`));
      }
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => resolve(Buffer.concat(chunks)));
      res.on('error', reject);
    }).on('error', reject);
  });
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
    const parsed = db.loadJobStateFromDB();
    if (parsed) {
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
  db.persistJobStateToDB(jobState);
}

function broadcastSSE(data, targetFilter = null) {
  for (const client of clients.values()) {
    if (client && client.res) {
      if (targetFilter) {
        const hasFilter = !!(targetFilter.userId || targetFilter.channelId);
        if (hasFilter) {
          const matchUser = targetFilter.userId && client.userId === targetFilter.userId;
          const matchChannel = targetFilter.channelId && client.channelId === targetFilter.channelId;
          if (!matchUser && !matchChannel) {
            continue; // Skip client: not their upload/job!
          }
        }
      }
      try {
        client.res.write(`data: ${JSON.stringify(data)}\n\n`);
      } catch (err) {
        // Client disconnected
      }
    }
  }
}

function addJobLog(message, level = 'info', targetFilter = null) {
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
  const filter = targetFilter || (jobState.ownerUserId || jobState.ownerChannelId ? { userId: jobState.ownerUserId, channelId: jobState.ownerChannelId } : null);
  broadcastSSE({ type: 'log', ...logItem }, filter);
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

// --- Server-Side OAuth Authentication ---

// 1. POST /api/auth/save-credentials
app.post('/api/auth/save-credentials', (req, res) => {
  try {
    const { clientId, clientSecret } = req.body;
    db.setSetting('oauth_client_id', clientId);
    db.setSetting('oauth_client_secret', clientSecret);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 2. GET /api/auth/url
app.get('/api/auth/url', (req, res) => {
  try {
    const clientIdRow = db.getSetting('oauth_client_id');
    const clientSecretRow = db.getSetting('oauth_client_secret');
    const clientId = clientIdRow?.value || '';
    const clientSecret = clientSecretRow?.value || '';
    
    if (!clientId || !clientSecret) {
      return res.status(400).json({ success: false, error: 'Client ID or Secret not configured' });
    }

    const redirectUri = `${req.protocol}://${req.get('host')}/api/auth/callback`;
    const oauth2Client = new google.auth.OAuth2(clientId, clientSecret, redirectUri);
    const authUrl = oauth2Client.generateAuthUrl({
      access_type: 'offline',
      prompt: 'consent',
      scope: [
        'https://www.googleapis.com/auth/drive.readonly',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/youtube.upload',
        'https://www.googleapis.com/auth/youtube.force-ssl',
        'https://www.googleapis.com/auth/youtube'
      ]
    });
    res.json({ success: true, url: authUrl });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 3. GET /api/auth/callback
app.get('/api/auth/callback', async (req, res) => {
  try {
    const code = req.query.code;
    const clientIdRow = db.getSetting('oauth_client_id');
    const clientSecretRow = db.getSetting('oauth_client_secret');
    const clientId = clientIdRow?.value || '';
    const clientSecret = clientSecretRow?.value || '';
    
    const redirectUri = `${req.protocol}://${req.get('host')}/api/auth/callback`;
    const oauth2Client = new google.auth.OAuth2(clientId, clientSecret, redirectUri);
    
    const { tokens } = await oauth2Client.getToken(code);
    if (tokens.refresh_token) {
      db.addCredential(clientId, clientSecret, tokens.refresh_token, 'Default');
    }
    
    if (tokens.access_token) {
      db.setSetting('oauth_access_token', tokens.access_token);
      db.setSetting('oauth_token_expiry', String(tokens.expiry_date || (Date.now() + 3600000)));
    }
    
    res.send('<html><body><script>window.opener ? window.opener.postMessage("auth_success","*") : null; window.location.href = "/?auth=success";</script><p>Authentication successful! Redirecting...</p></body></html>');
  } catch (err) {
    res.status(500).send(`Authentication failed: ${err.message}`);
  }
});

// 4. GET /api/auth/status
app.get('/api/auth/status', (req, res) => {
  try {
    const creds = db.getActiveCredentials();
    const hasRefreshToken = creds && creds.length > 0;
    res.json({ success: true, connected: hasRefreshToken, hasRefreshToken });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 5. POST /api/auth/revoke
app.post('/api/auth/revoke', (req, res) => {
  try {
    const creds = db.getActiveCredentials();
    if (creds && creds.length > 0) {
      creds.forEach(cred => db.removeCredential(cred.id));
    }
    db.setSetting('oauth_client_id', '');
    db.setSetting('oauth_client_secret', '');
    db.setSetting('oauth_access_token', '');
    db.setSetting('oauth_token_expiry', '');
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 7. getServerOAuth2Client()
function getServerOAuth2Client() {
  const creds = db.getActiveCredentials();
  if (creds && creds.length > 0) {
    const cred = creds[0];
    const clientId = cred.clientId;
    const clientSecret = cred.clientSecret;
    const oauth2Client = new google.auth.OAuth2(clientId, clientSecret);
    oauth2Client.setCredentials({ refresh_token: cred.refreshToken });
    return oauth2Client;
  }
  return null;
}

// 6. Modified getOAuth2Client
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
    const creds = db.getActiveCredentials();
    if (creds && creds.length > 0) {
      const cred = creds[0];
      const oauth2Client = new google.auth.OAuth2(cred.clientId, cred.clientSecret);
      oauth2Client.setCredentials({ refresh_token: cred.refreshToken });
      return oauth2Client;
    }
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
 * SSE Connection Endpoint (Per-User / Per-Channel Isolated)
 */
app.get('/api/events', async (req, res) => {
  const clientId = req.query.clientId || `client_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  const userId = req.query.userId || null;
  const token = req.query.token || null;
  let channelId = null;

  if (token) {
    try {
      if (tokenChannelCache.has(token)) {
        channelId = tokenChannelCache.get(token);
      } else {
        const { google } = require('googleapis');
        const oauth2 = new google.auth.OAuth2();
        oauth2.setCredentials({ access_token: token });
        const yt = google.youtube({ version: 'v3', auth: oauth2 });
        const chRes = await yt.channels.list({ part: ['id'], mine: true });
        channelId = chRes.data.items?.[0]?.id || null;
        if (channelId) tokenChannelCache.set(token, channelId);
      }
    } catch (e) {}
  }

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no'
  });

  clients.set(clientId, { res, req, userId, channelId, token });

  res.write(`data: ${JSON.stringify({ type: 'connected', clientId })}\n\n`);

  // Send initial scoped state
  const isJobActive = jobState.status === 'processing' || jobState.status === 'scanning' || jobState.status === 'uploading';
  const isMyJob = isJobActive && (
    (jobState.ownerUserId && jobState.ownerUserId === userId) ||
    (jobState.ownerChannelId && jobState.ownerChannelId === channelId)
  );

  if (isMyJob) {
    res.write(`data: ${JSON.stringify({ type: 'state_sync', state: jobState })}\n\n`);
  } else {
    const userHistory = (channelId || userId) ? db.getHistoryByUserOrChannel(channelId, userId) : [];
    const defaultState = getDefaultJobState();
    defaultState.files = userHistory;
    defaultState.stats = {
      total: userHistory.length,
      pending: 0,
      completed: userHistory.filter(f => f.status === 'completed').length,
      failed: userHistory.filter(f => f.status === 'failed').length
    };
    res.write(`data: ${JSON.stringify({ type: 'state_sync', state: defaultState })}\n\n`);
  }

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
  const userId = req.headers['x-user-id'] || req.query.userId || null;

  try {
    const channelId = await resolveChannelId(req);
    const userHistory = (channelId || userId) ? db.getHistoryByUserOrChannel(channelId, userId) : [];

    const isJobActive = jobState.status === 'processing' || jobState.status === 'scanning' || jobState.status === 'uploading';
    const isMyJob = isJobActive && (
      (jobState.ownerUserId && jobState.ownerUserId === userId) ||
      (jobState.ownerChannelId && jobState.ownerChannelId === channelId)
    );

    if (isMyJob) {
      res.json({ success: true, state: jobState, history: userHistory });
    } else {
      const userState = getDefaultJobState();
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
    res.json({ success: true, state: getDefaultJobState(), history: [] });
  }
});

app.get('/api/history', async (req, res) => {
  const userId = req.headers['x-user-id'] || req.query.userId || null;
  try {
    const channelId = await resolveChannelId(req);
    const userHistory = (channelId || userId) ? db.getHistoryByUserOrChannel(channelId, userId) : [];
    res.json({ success: true, history: userHistory });
  } catch (err) {
    res.json({ success: true, history: [] });
  }
});

/**
 * Real-Time API Quota & Health Engine Endpoint (Per-User / Per-Channel Scoped)
 * Calculates daily usage, remaining capacity, and exact countdown to 12:30 PM IST reset
 */
app.get('/api/quota-health', async (req, res) => {
  try {
    const channelId = await resolveChannelId(req);
    const userId = req.headers['x-user-id'] || req.query.userId || null;

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
      cycleStartUtc = new Date(todayResetIst.getTime() - istOffsetMs);
      const tomorrowResetIst = new Date(todayResetIst.getTime() + 24 * 3600 * 1000);
      nextResetUtc = new Date(tomorrowResetIst.getTime() - istOffsetMs);
    } else {
      const yesterdayResetIst = new Date(todayResetIst.getTime() - 24 * 3600 * 1000);
      cycleStartUtc = new Date(yesterdayResetIst.getTime() - istOffsetMs);
      nextResetUtc = new Date(todayResetIst.getTime() - istOffsetMs);
    }

    const cycleStartIso = cycleStartUtc.toISOString();
    const resetsInSeconds = Math.max(0, Math.floor((nextResetUtc.getTime() - now.getTime()) / 1000));

    // Count ONLY this user's uploads in current cycle (0 if no uploads or not logged in)
    const uploadsInCycle = (channelId || userId) ? db.getUploadsInCycle(cycleStartIso, channelId, userId) : 0;

    const keysCount = Math.max(1, parseInt(req.query.keysCount || '1', 10));
    const limitPerKey = 100;
    const totalDailyLimit = keysCount * limitPerKey;
    const usedCount = uploadsInCycle;
    const remainingCount = Math.max(0, totalDailyLimit - usedCount);
    const percentUsed = Math.min(100, Math.round((usedCount / totalDailyLimit) * 100));

    const isQuotaPaused = jobState.status === 'paused_quota' && (
      (jobState.ownerUserId && jobState.ownerUserId === userId) ||
      (jobState.ownerChannelId && jobState.ownerChannelId === channelId)
    );
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

app.post('/api/clear-history', async (req, res) => {
  const userId = req.headers['x-user-id'] || req.query.userId || null;
  const channelId = await resolveChannelId(req);
  const userFilter = (userId || channelId) ? { userId, channelId } : null;

  db.clearUserHistory(channelId, userId);

  // If this user has active files in jobState, clear only their files
  if (jobState.files && jobState.files.length > 0) {
    jobState.files = jobState.files.filter(f => {
      if (channelId && f.channelId === channelId) return false;
      if (userId && f.ownerUserId === userId) return false;
      return true;
    });
    jobState.stats = {
      total: jobState.files.length,
      pending: jobState.files.filter(f => f.status === 'queued' || f.status === 'uploading').length,
      completed: jobState.files.filter(f => f.status === 'completed').length,
      failed: jobState.files.filter(f => f.status === 'failed').length
    };
    persistJobState();
  }

  const defaultState = getDefaultJobState();
  broadcastSSE({ type: 'state_sync', state: defaultState }, userFilter);
  res.json({ success: true, message: 'Upload history cleared.' });
});

/**
 * Purge only Pending / Queued / Failed items from memory without touching completed uploads
 */
app.post('/api/clear-pending', async (req, res) => {
  const userId = req.headers['x-user-id'] || req.query.userId || null;
  const channelId = await resolveChannelId(req);
  const userFilter = (userId || channelId) ? { userId, channelId } : null;

  const isMyJob = (jobState.ownerUserId && jobState.ownerUserId === userId) ||
                  (jobState.ownerChannelId && jobState.ownerChannelId === channelId);

  let initialPending = 0;
  if (isMyJob) {
    initialPending = jobState.files.filter(f => f.status === 'queued' || f.status === 'failed' || f.status === 'uploading').length;
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
    broadcastSSE({ type: 'state_sync', state: jobState }, userFilter);
  }
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
      await runUploadQueue(auth);
    } catch (err) {
      console.error('Error during retry-pending queue:', err);
    }
  })();
});

/**
 * Real-Time Video Title Update Endpoint (Pre-upload or Live YouTube)
 */
app.post('/api/update-title', async (req, res) => {
  const { fileId, newTitle } = req.body;

  if (!fileId || !newTitle || !newTitle.trim()) {
    return res.status(400).json({ success: false, error: 'File ID and a valid title are required.' });
  }

  const trimmedTitle = sanitizeYouTubeTitle(newTitle);
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
            buffer = await fetchUrlAsBuffer(imageUrl);
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
      const trimmedTitle = sanitizeYouTubeTitle(title);
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
                buffer = await fetchUrlAsBuffer(thumbnailUrl);
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
              newThumb = (item.maxres || item.standard || item.high || item.medium || item.default)?.url || `https://i.ytimg.com/vi/${targetVideoId}/hqdefault.jpg?t=${Date.now()}`;
            } else {
              newThumb = `https://i.ytimg.com/vi/${targetVideoId}/hqdefault.jpg?t=${Date.now()}`;
            }
            fileObj.thumbnailUrl = newThumb;
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

// ─── Settings API ────────────────────────────────────────────────────────────
app.get('/api/settings', (req, res) => {
  res.json({ success: true, settings: db.getAllSettings() });
});

app.post('/api/settings', (req, res) => {
  const { key, value } = req.body;
  if (!key) return res.status(400).json({ success: false, error: 'Setting key is required.' });
  db.setSetting(key, value);
  res.json({ success: true, message: `Setting '${key}' updated.` });
});

app.get('/api/settings/credentials', (req, res) => {
  res.json({ success: true, credentials: db.getAllCredentials() });
});

app.post('/api/settings/credentials', (req, res) => {
  const { clientId, clientSecret, refreshToken, label } = req.body;
  if (!clientId || !clientSecret || !refreshToken) {
    return res.status(400).json({ success: false, error: 'clientId, clientSecret, and refreshToken are required.' });
  }
  db.addCredential(clientId, clientSecret, refreshToken, label || 'New Key');
  res.json({ success: true, message: 'Credential added successfully.' });
});

app.delete('/api/settings/credentials/:id', (req, res) => {
  db.removeCredential(parseInt(req.params.id, 10));
  res.json({ success: true, message: 'Credential removed.' });
});

// In-memory TTL cache for Drive folder scan results (60s)
const folderScanCache = new Map();
function getCachedScan(folderId, startDate, endDate) {
  const key = `${folderId}:${startDate || ''}:${endDate || ''}`;
  const cached = folderScanCache.get(key);
  if (cached && Date.now() < cached.expiresAt) return cached.data;
  folderScanCache.delete(key);
  return null;
}
function setCachedScan(folderId, startDate, endDate, data) {
  const key = `${folderId}:${startDate || ''}:${endDate || ''}`;
  folderScanCache.set(key, { data, expiresAt: Date.now() + 60000 });
}

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
      let scanResult = getCachedScan(fId, startDateIso, endDateIso);
      if (!scanResult) {
        scanResult = await scanDriveFolderRecursively(drive, fId, startDateIso, endDateIso);
        setCachedScan(fId, startDateIso, endDateIso, scanResult);
      }
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

      const duplicateCheck = db.isDuplicate(f.id, combinedTitle, f.name);
      const isDuplicate = duplicateCheck.isDuplicate;
      const existingRecord = duplicateCheck.existing;

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

  const userId = req.headers['x-user-id'] || req.body.userId || null;
  let channelId = null;
  try {
    channelId = await resolveChannelId(req);
  } catch (e) {}
  activeJobChannelId = channelId;

  jobState = {
    id: `job_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`,
    ownerUserId: userId,
    ownerChannelId: channelId,
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

  const userFilter = (userId || channelId) ? { userId, channelId } : null;
  broadcastSSE({ type: 'state_sync', state: jobState }, userFilter);

  // RUN BACKGROUND PIPELINE
  (async () => {
    activeAbortController = new AbortController();

    try {
      const drive = google.drive({ version: 'v3', auth });
      const youtube = google.youtube({ version: 'v3', auth });

      addJobLog(`Scanning Google Drive Folder(s) & subfolders recursively...`, 'info', userFilter);

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
        let scanResult = getCachedScan(fId, startDateIso, endDateIso);
      if (!scanResult) {
        scanResult = await scanDriveFolderRecursively(drive, fId, startDateIso, endDateIso);
        setCachedScan(fId, startDateIso, endDateIso, scanResult);
      }
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
    const rawTitle = (req.query.title || 'Direct Lecture Video').trim();
    const title = sanitizeYouTubeTitle(rawTitle);
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

    const channelId = await resolveChannelId(req);
    const userId = req.headers['x-user-id'] || req.query.userId || null;
    const userFilter = (userId || channelId) ? { userId, channelId } : null;

    const record = {
      id: videoId,
      videoId: videoId,
      name: title,
      originalName: title,
      customTitle: title,
      batch,
      subject,
      folderPath: 'Manual Device Upload',
      channelId,
      ownerUserId: userId,
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
    }, userFilter);

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
    const initPayload = JSON.stringify({
      snippet: {
        title,
        description: fullDesc,
        tags,
        categoryId: '27'
      },
      status: statusConfig
    });

    const initReq = https.request('https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json; charset=UTF-8',
        'X-Upload-Content-Type': req.body.mimeType || 'video/mp4',
        'X-Upload-Content-Length': req.body.fileSize || 0
      }
    }, (initRes) => {
      const location = initRes.headers.location;
      if (initRes.statusCode >= 200 && initRes.statusCode < 300 && location) {
        return res.json({
          success: true,
          uploadUrl: location,
          message: 'Direct upload initiated successfully.'
        });
      }

      let errData = '';
      initRes.on('data', d => { errData += d; });
      initRes.on('end', () => {
        let parsed = errData;
        try { parsed = JSON.parse(errData); } catch (e) {}
        console.error('YouTube direct upload init error:', parsed);
        return res.status(initRes.statusCode || 500).json({
          success: false,
          error: parsed.error?.message || errData || 'Could not initiate YouTube direct upload.'
        });
      });
    });

    initReq.on('error', (err) => {
      console.error('YouTube direct upload request error:', err);
      return res.status(500).json({ success: false, error: err.message });
    });

    initReq.write(initPayload);
    initReq.end();
  } catch (err) {
    console.error('Direct upload initiate fatal:', err);
    return res.status(500).json({ success: false, error: err.message });
  }
});

/**
 * Complete Direct Upload & Apply Metadata, Thumbnail, and Playlists
 */
app.post('/api/complete-direct-upload', async (req, res) => {
  const { videoId, title, batch, subject, playlistName, thumbnailUrl, thumbnailBase64, fileSize } = req.body;
  const auth = getOAuth2Client(req);
  const channelId = await resolveChannelId(req);
  const userId = req.headers['x-user-id'] || req.body.userId || null;
  const userFilter = (userId || channelId) ? { userId, channelId } : null;

  if (!videoId) {
    return res.status(400).json({ success: false, error: 'videoId is required to finalize direct upload.' });
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
    channelId,
    ownerUserId: userId,
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
          addJobLog(`✔ Added "${title}" to Playlist: "${playlistName}"`, 'info', userFilter);
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
        addJobLog(`✔ Branded thumbnail uploaded for "${title}"`, 'success', userFilter);
      } catch (tErr) {
        console.warn('Manual upload thumbnail set error:', tErr.message);
      }
    }
  }

  saveCompletedFileToHistory(record);
  addJobLog(`✔ Manual Upload Complete: "${title}" ➔ ${youtubeUrl}`, 'success', userFilter);

  broadcastSSE({
    type: 'file_completed',
    fileId: videoId,
    fileName: title,
    videoId,
    youtubeUrl,
    studioUrl,
    thumbnailUrl: finalThumbnail
  }, userFilter);

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
  console.log(` Database: ${path.join(DATA_DIR, 'app.db')}         `);
  console.log(`====================================================`);
});

// ─── Concurrency Limiter ─────────────────────────────────────────────────────
function createLimiter(concurrency) {
  let active = 0;
  const queue = [];
  function next() {
    while (active < concurrency && queue.length > 0) {
      active++;
      const { fn, resolve, reject } = queue.shift();
      fn().then(resolve, reject).finally(() => { active--; next(); });
    }
  }
  return function limit(fn) {
    return new Promise((resolve, reject) => {
      queue.push({ fn, resolve, reject });
      next();
    });
  };
}

// ─── Retry & Quota Helpers ───────────────────────────────────────────────────
const RETRY_DELAYS = [2000, 8000, 30000]; // 2s, 8s, 30s exponential backoff

function isQuotaError(err) {
  const msg = (err.message || '').toLowerCase();
  return (
    msg.includes('exceeded the number of videos') ||
    msg.includes('uploadlimitexceeded') ||
    msg.includes('quota') ||
    msg.includes('daily upload') ||
    err.code === 403 ||
    (err.code === 400 && (msg.includes('upload') || msg.includes('limit') || msg.includes('exceeded')))
  );
}

function scheduleQuotaResume(auth) {
  const now = new Date();
  const istOffsetMs = (5 * 60 + 30) * 60 * 1000;
  const nowIst = new Date(now.getTime() + istOffsetMs);
  const todayReset = new Date(nowIst);
  todayReset.setUTCHours(12, 30, 0, 0);
  let nextReset = todayReset;
  if (nowIst >= todayReset) {
    nextReset = new Date(todayReset.getTime() + 24 * 3600 * 1000);
  }
  const msUntilReset = new Date(nextReset.getTime() - istOffsetMs).getTime() - now.getTime();
  const resumeMs = Math.max(msUntilReset + 60000, 60000); // +1 min buffer

  addJobLog(`Quota exhausted. Remaining uploads scheduled for auto-resume in ${Math.round(resumeMs / 60000)} minutes.`, 'warn');

  setTimeout(async () => {
    // Reset credential quotas
    db.resetAllCredentialQuotas();

    // Move scheduled_for_tomorrow → queued
    jobState.files.forEach(f => {
      if (f.status === 'scheduled_for_tomorrow') f.status = 'queued';
    });
    jobState.status = 'processing';
    persistJobState();
    addJobLog('Quota reset detected — auto-resuming scheduled uploads.', 'highlight');
    broadcastSSE({ type: 'state_sync', state: jobState });

    // Try to get auth from stored credentials
    try {
      const nextAuth = await getNextAvailableAuth();
      if (nextAuth) {
        await runUploadQueue(nextAuth.auth, nextAuth.credentialId);
        if (jobState.status !== 'cancelled' && jobState.status !== 'paused_quota') {
          jobState.status = 'completed';
          jobState.finishedAt = new Date().toISOString();
          addJobLog('All scheduled uploads completed successfully after quota reset.', 'success');
          broadcastSSE({ type: 'process_completed', message: 'All videos processed successfully.' });
          persistJobState();
        }
      } else {
        addJobLog('No valid credentials available for auto-resume. Please connect Google account manually.', 'error');
        broadcastSSE({ type: 'auth_required', message: 'Auto-resume failed: no stored credentials. Please re-authorize.' });
      }
    } catch (err) {
      console.error('Auto-resume error:', err);
      addJobLog('Auto-resume failed: ' + err.message, 'error');
    }
  }, resumeMs);
}

async function getNextAvailableAuth() {
  const creds = db.getActiveCredentials(); // Ordered by quotaUsedToday ASC
  for (const cred of creds) {
    if (cred.quotaUsedToday < 100) {
      try {
        const { google } = require('googleapis');
        const oauth2 = new google.auth.OAuth2(cred.clientId, cred.clientSecret);
        oauth2.setCredentials({ refresh_token: cred.refreshToken });
        // Verify token works by refreshing
        await oauth2.getAccessToken();
        return { auth: oauth2, credentialId: cred.id };
      } catch (err) {
        console.warn(`Credential ${cred.label} (id=${cred.id}) failed auth: ${err.message}`);
        continue;
      }
    }
  }
  return null;
}

// ─── Single File Upload with Retry ───────────────────────────────────────────
async function uploadSingleFile(drive, youtube, auth, fileObj, index, total, credentialId) {
  const uploadTitle = sanitizeYouTubeTitle(fileObj.customTitle || fileObj.name || fileObj.originalName);
  fileObj.status = 'uploading';
  fileObj.channelId = activeJobChannelId || fileObj.channelId || null;
  fileObj.ownerUserId = jobState.ownerUserId || fileObj.ownerUserId || null;
  persistJobState();

  const userFilter = (jobState.ownerUserId || activeJobChannelId) ? { userId: jobState.ownerUserId, channelId: activeJobChannelId } : null;

  addJobLog(`[${index + 1}/${total}] Streaming: "${uploadTitle}" (${fileObj.subject})`, 'highlight', userFilter);
  broadcastSSE({
    type: 'file_start',
    fileId: fileObj.id,
    fileName: uploadTitle,
    subject: fileObj.subject,
    batch: fileObj.batch,
    index: index + 1,
    total: total,
    totalBytes: fileObj.totalBytes
  }, userFilter);

  let lastError = null;

  for (let attempt = 0; attempt <= RETRY_DELAYS.length; attempt++) {
    if (jobState.status === 'cancelled' || jobState.status === 'paused_quota') return;

    try {
      await executeFileUpload(drive, youtube, auth, fileObj, uploadTitle, credentialId, userFilter);
      return; // Success
    } catch (err) {
      lastError = err;
      console.error(`Error processing ${uploadTitle} (attempt ${attempt + 1}):`, err.message);

      if (isQuotaError(err)) {
        // Quota error — don't retry, handle at queue level
        fileObj.status = 'failed';
        fileObj.error = 'YouTube daily limit reached (10-15 videos/day for channel). Click "Use Drive Player" for instant playback.';

        jobState.stats.pending = Math.max(0, jobState.stats.pending - 1);
        jobState.stats.failed += 1;

        addJobLog(`YouTube API Daily Upload Limit reached. Paused remaining uploads.`, 'warn', userFilter);
        persistJobState();

        broadcastSSE({
          type: 'quota_exceeded',
          fileId: fileObj.id,
          fileName: uploadTitle,
          error: fileObj.error,
          message: 'YouTube daily upload limit reached. You can convert remaining videos to Secure Drive Player instantly.'
        }, userFilter);

        // Mark remaining queued files as scheduled_for_tomorrow
        jobState.status = 'paused_quota';
        jobState.files.forEach(f => {
          if (f.status === 'queued') f.status = 'scheduled_for_tomorrow';
        });
        persistJobState();

        // Try rotating to next credential
        const nextAuth = await getNextAvailableAuth();
        if (nextAuth) {
          addJobLog(`Rotating to next API credential: ${db.getCredentialById(nextAuth.credentialId)?.label || 'Unknown'}`, 'highlight', userFilter);
          jobState.files.forEach(f => {
            if (f.status === 'scheduled_for_tomorrow') f.status = 'queued';
          });
          jobState.status = 'processing';
          persistJobState();
          broadcastSSE({ type: 'state_sync', state: jobState }, userFilter);
          // The caller (runUploadQueue) will detect credential rotation
          throw Object.assign(new Error('CREDENTIAL_ROTATION'), { nextAuth });
        }

        // No more credentials — schedule for tomorrow
        scheduleQuotaResume(auth);
        throw err; // Propagate to stop the queue
      }

      if (attempt < RETRY_DELAYS.length) {
        const delay = RETRY_DELAYS[attempt];
        addJobLog(`Retry ${attempt + 1}/3 for "${uploadTitle}" in ${delay / 1000}s...`, 'warn', userFilter);
        broadcastSSE({ type: 'file_retry', fileId: fileObj.id, fileName: uploadTitle, attempt: attempt + 1, delayMs: delay }, userFilter);
        await new Promise(r => setTimeout(r, delay));
        fileObj.status = 'uploading';
        fileObj.percentage = 0;
        fileObj.uploadedBytes = 0;
      }
    }
  }

  // All retries exhausted
  fileObj.status = 'failed';
  fileObj.error = lastError?.message || 'Upload failed after 3 retries';
  jobState.stats.pending = Math.max(0, jobState.stats.pending - 1);
  jobState.stats.failed += 1;
  addJobLog(`Failed to upload "${uploadTitle}" after 3 retries: ${fileObj.error}`, 'error', userFilter);
  persistJobState();
  broadcastSSE({ type: 'file_error', fileId: fileObj.id, fileName: uploadTitle, error: fileObj.error }, userFilter);
}

// ─── Execute Single Upload (Drive → YouTube stream) ──────────────────────────
async function executeFileUpload(drive, youtube, auth, fileObj, uploadTitle, credentialId, userFilter = null) {
  const { Transform } = require('stream');

  fileObj.channelId = activeJobChannelId || fileObj.channelId || null;
  fileObj.ownerUserId = jobState.ownerUserId || fileObj.ownerUserId || null;

  if (jobState.processingMode === 'drive_secure') {
    // Drive Secure mode — just set permissions
    fileObj.percentage = 10;
    broadcastSSE({
      type: 'upload_progress', fileId: fileObj.id, fileName: uploadTitle,
      uploadedBytes: 0, totalBytes: fileObj.totalBytes, percentage: 10, speedMBps: 0, etaSeconds: 0
    }, userFilter);

    fileObj.percentage = 50;
    broadcastSSE({
      type: 'upload_progress', fileId: fileObj.id, fileName: uploadTitle,
      uploadedBytes: 0, totalBytes: fileObj.totalBytes, percentage: 50, speedMBps: 0, etaSeconds: 0
    }, userFilter);

    try {
      await drive.permissions.create({
        fileId: fileObj.id,
        requestBody: { role: 'reader', type: 'anyone' },
        supportsAllDrives: true
      });
    } catch (permErr) {
      if (!permErr.message?.includes('already has access')) throw permErr;
    }

    const embedUrl = `https://drive.google.com/file/d/${fileObj.id}/preview`;
    fileObj.status = 'completed';
    fileObj.percentage = 100;
    fileObj.videoId = fileObj.id;
    fileObj.youtubeUrl = embedUrl;
    fileObj.studioUrl = '';
    fileObj.thumbnailUrl = `https://drive.google.com/thumbnail?id=${fileObj.id}&sz=w320`;

    jobState.stats.pending = Math.max(0, jobState.stats.pending - 1);
    jobState.stats.completed += 1;

    saveCompletedFileToHistory(fileObj);
    persistJobState();
    broadcastSSE({
      type: 'file_completed', fileId: fileObj.id, fileName: uploadTitle,
      videoId: fileObj.videoId, youtubeUrl: embedUrl,
      studioUrl: '', thumbnailUrl: fileObj.thumbnailUrl
    }, userFilter);
    return;
  }

  // YouTube Standard mode — stream from Drive to YouTube
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
    highWaterMark: 2 * 1024 * 1024, // 2MB buffer for reduced chunking overhead
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

      // Throttle SSE to max 1 event per 500ms per file
      if ((percent !== lastReportedPercent && (currentTime - lastReportTime >= 500 || percent === 100)) || uploadedBytes === chunk.length) {
        lastReportedPercent = percent;
        lastReportTime = currentTime;
        broadcastSSE({
          type: 'upload_progress',
          fileId: fileObj.id, fileName: uploadTitle,
          uploadedBytes, totalBytes: fileObj.totalBytes,
          percentage: percent, speedMBps: fileObj.speedMBps, etaSeconds: fileObj.etaSeconds
        }, userFilter);
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
  };
  if (isScheduled) {
    videoStatus.publishAt = jobState.scheduledPublishAt;
  }

  const insertResponse = await youtube.videos.insert({
    part: ['snippet', 'status'],
    requestBody: {
      snippet: {
        title: uploadTitle,
        description: `${fileObj.batch || 'Batch'} — ${fileObj.subject || 'Lecture'}\nUploaded via Drive2YT Pipeline`,
        tags: [fileObj.batch, fileObj.subject, 'lecture', 'education'].filter(Boolean),
        categoryId: '27'
      },
      status: videoStatus
    },
    media: { body: monitoredStream }
  });

  const videoId = insertResponse.data.id;
  const youtubeUrl = `https://youtu.be/${videoId}`;
  const studioUrl = `https://studio.youtube.com/video/${videoId}/edit`;
  let thumbnailUrl = `https://img.youtube.com/vi/${videoId}/mqdefault.jpg`;

  // Increment credential quota counter
  if (credentialId) {
    db.incrementCredentialQuota(credentialId);
  }

  // Custom thumbnail upload
  const customThumbData = jobState.customThumbnails?.[fileObj.id];
  if (customThumbData) {
    try {
      const thumbBuf = Buffer.from(customThumbData, 'base64');
      await youtube.thumbnails.set({
        videoId: videoId,
        media: { mimeType: 'image/jpeg', body: require('stream').Readable.from(thumbBuf) }
      });
      addJobLog(`Custom thumbnail applied for "${uploadTitle}"`, 'info', userFilter);
    } catch (thumbErr) {
      addJobLog(`Thumbnail upload failed for "${uploadTitle}": ${thumbErr.message}`, 'warn', userFilter);
    }
  }

  // Playlist insertion
  if (jobState.playlistId) {
    try {
      await addVideoToPlaylist(youtube, jobState.playlistId, videoId);
    } catch (plErr) {
      addJobLog(`Playlist insert error for "${uploadTitle}": ${plErr.message}`, 'warn', userFilter);
    }
  }

  fileObj.status = 'completed';
  fileObj.percentage = 100;
  fileObj.videoId = videoId;
  fileObj.youtubeUrl = youtubeUrl;
  fileObj.studioUrl = studioUrl;
  fileObj.thumbnailUrl = thumbnailUrl;

  jobState.stats.pending = Math.max(0, jobState.stats.pending - 1);
  jobState.stats.completed += 1;

  saveCompletedFileToHistory(fileObj);
  persistJobState();

  addJobLog(`✔ Uploaded "${uploadTitle}" → ${youtubeUrl}`, 'success', userFilter);
  broadcastSSE({
    type: 'file_completed', fileId: fileObj.id, fileName: uploadTitle,
    videoId, youtubeUrl, studioUrl, thumbnailUrl
  }, userFilter);
}

// ─── Concurrent Upload Queue ─────────────────────────────────────────────────
async function runUploadQueue(auth, credentialId) {
  const concurrency = parseInt(db.getSetting('upload_concurrency') || '3', 10);
  const limit = createLimiter(Math.max(1, Math.min(concurrency, 10)));
  const { google } = require('googleapis');
  const drive = google.drive({ version: 'v3', auth });
  const youtube = google.youtube({ version: 'v3', auth });

  const pendingFiles = jobState.files.filter(f =>
    f.status !== 'completed' && f.status !== 'failed' && f.status !== 'scheduled_for_tomorrow'
  );

  if (pendingFiles.length === 0) return;

  addJobLog(`Starting concurrent upload queue (${pendingFiles.length} files, concurrency=${concurrency})`, 'highlight');

  const uploadTasks = pendingFiles.map((fileObj, idx) =>
    limit(async () => {
      if (jobState.status === 'cancelled' || jobState.status === 'paused_quota') return;
      try {
        await uploadSingleFile(drive, youtube, auth, fileObj, idx, pendingFiles.length, credentialId);
      } catch (err) {
        if (err.message === 'CREDENTIAL_ROTATION' && err.nextAuth) {
          // Credential rotation — restart queue with new auth
          addJobLog('Restarting upload queue with rotated credentials...', 'highlight');
          await runUploadQueue(err.nextAuth.auth, err.nextAuth.credentialId);
        }
        // Other errors are already handled in uploadSingleFile
      }
    })
  );

  await Promise.allSettled(uploadTasks);
}
