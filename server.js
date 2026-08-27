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

const app = express();
const PORT = process.env.PORT || 3000;
const DATA_DIR = path.join(__dirname, 'data');
const STATE_FILE = path.join(DATA_DIR, 'job_state.json');

if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Store active SSE client connections
const clients = new Map();

// Full Scopes for Uploading, Updating Metadata, and Playlist Management
const SCOPES = [
  'https://www.googleapis.com/auth/drive.readonly',
  'https://www.googleapis.com/auth/youtube.upload',
  'https://www.googleapis.com/auth/youtube.force-ssl',
  'https://www.googleapis.com/auth/youtube'
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
    status: 'idle', // 'idle' | 'scanning' | 'processing' | 'completed' | 'cancelled' | 'error'
    startedAt: null,
    finishedAt: null,
    files: [],
    logs: [],
    stats: { total: 0, pending: 0, completed: 0, failed: 0 }
  };
}

function loadJobState() {
  try {
    if (fs.existsSync(STATE_FILE)) {
      const data = fs.readFileSync(STATE_FILE, 'utf8');
      const parsed = JSON.parse(data);
      if (parsed.status === 'processing' || parsed.status === 'scanning') {
        parsed.status = 'error';
        parsed.logs.push({
          timestamp: new Date().toISOString(),
          message: 'Server restarted while job was in progress.',
          level: 'warn'
        });
      }
      return parsed;
    }
  } catch (err) {
    console.error('Error reading job state:', err);
  }
  return getDefaultJobState();
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

function getOAuth2Client() {
  const clientId = process.env.GOOGLE_CLIENT_ID;
  const clientSecret = process.env.GOOGLE_CLIENT_SECRET;
  const redirectUri = process.env.GOOGLE_REDIRECT_URI || `http://localhost:${PORT}/oauth2callback`;
  const refreshToken = process.env.GOOGLE_REFRESH_TOKEN;

  if (!clientId || !clientSecret) {
    return null;
  }

  const oauth2Client = new google.auth.OAuth2(clientId, clientSecret, redirectUri);
  if (refreshToken) {
    oauth2Client.setCredentials({ refresh_token: refreshToken });
  }
  return oauth2Client;
}

/**
 * 1-Click Auth Initiation Route
 */
app.get('/auth/google', (req, res) => {
  const oauth2Client = getOAuth2Client();
  if (!oauth2Client) {
    return res.status(500).send('GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET must be configured in .env');
  }

  const authUrl = oauth2Client.generateAuthUrl({
    access_type: 'offline',
    prompt: 'consent',
    scope: SCOPES
  });

  res.redirect(authUrl);
});

/**
 * OAuth2 Callback Route
 */
app.get('/oauth2callback', async (req, res) => {
  const { code, error } = req.query;

  if (error) {
    return res.redirect(`/?auth=error&message=${encodeURIComponent(error)}`);
  }

  if (!code) {
    return res.redirect('/?auth=error&message=No+authorization+code+received');
  }

  try {
    const oauth2Client = getOAuth2Client();
    const { tokens } = await oauth2Client.getToken(code);

    if (tokens.refresh_token) {
      updateEnvFile('GOOGLE_REFRESH_TOKEN', tokens.refresh_token);
      console.log('✔ Refresh token obtained and saved to .env successfully with full permissions.');
    }
    res.redirect('/?auth=success');
  } catch (err) {
    console.error('OAuth callback exchange error:', err);
    res.redirect(`/?auth=error&message=${encodeURIComponent(err.message)}`);
  }
});

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

/**
 * Helper: Recursive Drive Scanner with Batch & Subject Hierarchy Tracking and Date Range Filtering
 */
async function scanDriveFolderRecursively(drive, rootFolderId, startDateIso, endDateIso) {
  const discoveredVideos = new Map();
  let rootFolderName = null;

  try {
    const rootMeta = await drive.files.get({
      fileId: rootFolderId,
      fields: 'id, name',
      supportsAllDrives: true
    });
    rootFolderName = rootMeta.data.name;
  } catch (e) {
    rootFolderName = 'Batch Folder';
  }

  // Queue stores objects: { folderId, folderPath, subject }
  const folderQueue = [{
    folderId: rootFolderId,
    folderPath: rootFolderName || 'Root',
    subject: 'General'
  }];
  const visitedFolders = new Set();

  while (folderQueue.length > 0) {
    const current = folderQueue.shift();
    if (visitedFolders.has(current.folderId)) continue;
    visitedFolders.add(current.folderId);

    // A. Query videos in this folder within the selected Date Range
    let dateFilterClause = '';
    if (startDateIso && endDateIso) {
      dateFilterClause = `and createdTime >= '${startDateIso}' and createdTime <= '${endDateIso}'`;
    } else if (startDateIso) {
      dateFilterClause = `and createdTime >= '${startDateIso}'`;
    }

    const videoQuery = `'${current.folderId}' in parents and mimeType contains 'video/' and trashed = false ${dateFilterClause}`.trim();
    try {
      const videoRes = await drive.files.list({
        q: videoQuery,
        fields: 'files(id, name, mimeType, size, createdTime)',
        orderBy: 'createdTime asc',
        supportsAllDrives: true,
        includeItemsFromAllDrives: true
      });

      const files = videoRes.data.files || [];
      for (const file of files) {
        if (!discoveredVideos.has(file.id)) {
          discoveredVideos.set(file.id, {
            ...file,
            batch: rootFolderName || 'Batch',
            subject: current.subject,
            folderPath: current.folderPath
          });
        }
      }
    } catch (err) {
      console.warn(`Video query error in folder ${current.folderId}:`, err.message);
    }

    // B. Query subfolders (Physics, Chemistry, Botany, Zoology, etc.)
    const subfolderQuery = `'${current.folderId}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false`;
    try {
      const subfolderRes = await drive.files.list({
        q: subfolderQuery,
        fields: 'files(id, name)',
        supportsAllDrives: true,
        includeItemsFromAllDrives: true
      });

      const subfolders = subfolderRes.data.files || [];
      for (const sub of subfolders) {
        if (!visitedFolders.has(sub.id)) {
          folderQueue.push({
            folderId: sub.id,
            folderPath: `${current.folderPath} / ${sub.name}`,
            subject: sub.name // Subfolder is the subject (e.g. Physics, Chemistry)
          });
        }
      }
    } catch (err) {
      console.warn(`Subfolder query error in folder ${current.folderId}:`, err.message);
    }
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

app.get('/api/job-status', (req, res) => {
  res.json({ success: true, state: jobState });
});

app.get('/api/auth-status', (req, res) => {
  const hasClientId = !!process.env.GOOGLE_CLIENT_ID;
  const hasClientSecret = !!process.env.GOOGLE_CLIENT_SECRET;
  const hasRefreshToken = !!process.env.GOOGLE_REFRESH_TOKEN;

  res.json({
    configured: hasClientId && hasClientSecret && hasRefreshToken,
    missing: [
      !hasClientId && 'GOOGLE_CLIENT_ID',
      !hasClientSecret && 'GOOGLE_CLIENT_SECRET',
      !hasRefreshToken && 'GOOGLE_REFRESH_TOKEN'
    ].filter(Boolean)
  });
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

  const auth = getOAuth2Client();

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
 * Cancel Running Job Endpoint
 */
app.post('/api/cancel', (req, res) => {
  if (jobState.status === 'processing' || jobState.status === 'scanning') {
    if (activeAbortController) {
      activeAbortController.abort();
    }
    jobState.status = 'cancelled';
    addJobLog('Upload pipeline was manually cancelled by user.', 'warn');
    broadcastSSE({ type: 'job_cancelled', message: 'Job was cancelled.' });
    persistJobState();
    return res.json({ success: true, message: 'Job cancelled successfully.' });
  }
  return res.json({ success: false, message: 'No active job is currently running.' });
});

/**
 * Reset Job State Endpoint (Only clears inputs/queue, preserves completed video history)
 */
app.post('/api/reset', (req, res) => {
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
 * Core Processing Endpoint
 */
app.post('/api/process', async (req, res) => {
  const { folderInput, playlistName, startDate, endDate } = req.body;

  if (jobState.status === 'processing' || jobState.status === 'scanning') {
    return res.status(400).json({
      success: false,
      error: 'A video upload job is already running in the background.'
    });
  }

  if (!folderInput) {
    return res.status(400).json({ success: false, error: 'Google Drive Folder link or ID is required.' });
  }

  const folderIds = extractFolderIds(folderInput);
  if (folderIds.length === 0) {
    return res.status(400).json({ success: false, error: 'Invalid Google Drive Folder link or ID format.' });
  }

  const auth = getOAuth2Client();
  if (!auth || !process.env.GOOGLE_REFRESH_TOKEN) {
    return res.status(500).json({
      success: false,
      error: 'Google OAuth2 is not connected. Please click "Connect Google" first.'
    });
  }

  jobState = {
    id: `job_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`,
    folderIds,
    folderInput,
    playlistTitle: (playlistName || '').trim(),
    playlistId: null,
    playlistUrl: null,
    status: 'scanning',
    startedAt: new Date().toISOString(),
    finishedAt: null,
    files: [],
    logs: [],
    stats: { total: 0, pending: 0, completed: 0, failed: 0 }
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

      addJobLog(`Scanning Google Drive Folder(s) & subfolders recursively...`);

      // Determine Date Range
      const now = new Date();
      let startObj, endObj;

      if (startDate) {
        startObj = new Date(startDate + 'T00:00:00');
        if (isNaN(startObj.getTime())) {
          startObj = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0, 0);
        }
      } else {
        startObj = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0, 0);
      }

      if (endDate) {
        endObj = new Date(endDate + 'T23:59:59.999');
        if (isNaN(endObj.getTime())) {
          endObj = new Date(startObj.getFullYear(), startObj.getMonth(), startObj.getDate(), 23, 59, 59, 999);
        }
      } else {
        endObj = new Date(startObj.getFullYear(), startObj.getMonth(), startObj.getDate(), 23, 59, 59, 999);
      }

      const startDateIso = startObj.toISOString();
      const endDateIso = endObj.toISOString();

      addJobLog(`Filtering videos created between: ${startObj.toLocaleDateString()} and ${endObj.toLocaleDateString()}`);

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

      if (rawFiles.length === 0) {
        jobState.status = 'completed';
        jobState.finishedAt = new Date().toISOString();
        addJobLog('No video files found created today in the given folder or its subfolders.', 'warn');
        broadcastSSE({ type: 'no_files_found', message: 'No video files found created today.' });
        persistJobState();
        return;
      }

      // Setup Playlist Name
      const targetPlaylistTitle = jobState.playlistTitle || autoDetectedFolderName || 'Unlisted Uploads';
      jobState.playlistTitle = targetPlaylistTitle;

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

      jobState.status = 'processing';
      jobState.files = rawFiles.map((f, idx) => {
        return {
          index: idx + 1,
          id: f.id,
          name: f.name,
          originalName: f.name,
          customTitle: f.name,
          batch: f.batch || autoDetectedFolderName || 'Batch',
          subject: f.subject || 'Lecture',
          folderPath: f.folderPath || '',
          size: parseInt(f.size || '0', 10),
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

      jobState.stats = {
        total: jobState.files.length,
        pending: jobState.files.length,
        completed: 0,
        failed: 0
      };

      addJobLog(`Discovered ${jobState.files.length} video(s) created today. Starting stream queue...`, 'highlight');
      persistJobState();
      broadcastSSE({ type: 'state_sync', state: jobState });

      for (let i = 0; i < jobState.files.length; i++) {
        if (jobState.status === 'cancelled') {
          addJobLog('Pipeline cancelled during queue execution.', 'warn');
          break;
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

          const ytResponse = await youtube.videos.insert({
            part: ['snippet', 'status'],
            requestBody: {
              snippet: {
                title: uploadTitle,
                description: `Lecture Video: ${uploadTitle}\nBatch: ${fileObj.batch}\nSubject: ${fileObj.subject}\nPlaylist: ${jobState.playlistTitle}\nUploaded on: ${new Date().toISOString()}`,
                tags: ['DriveToYouTube', 'AutomatedUpload', fileObj.subject, fileObj.batch],
                categoryId: '27' // Education
              },
              status: {
                privacyStatus: 'unlisted',
                selfDeclaredMadeForKids: false
              }
            },
            media: {
              body: monitoredStream
            }
          });

          const videoId = ytResponse.data.id;
          const youtubeUrl = `https://youtu.be/${videoId}`;
          const studioUrl = `https://studio.youtube.com/video/${videoId}/edit`;
          const thumbnailUrl = `https://img.youtube.com/vi/${videoId}/mqdefault.jpg`;

          fileObj.status = 'completed';
          fileObj.percentage = 100;
          fileObj.videoId = videoId;
          fileObj.youtubeUrl = youtubeUrl;
          fileObj.studioUrl = studioUrl;
          fileObj.thumbnailUrl = thumbnailUrl;

          // Add to Playlist
          if (jobState.playlistId) {
            try {
              await addVideoToPlaylist(youtube, jobState.playlistId, videoId);
              addJobLog(`✔ Added "${uploadTitle}" to Playlist: "${jobState.playlistTitle}"`, 'info');
            } catch (plErr) {
              console.warn('Playlist item insert error:', plErr.message);
            }
          }

          jobState.stats.pending = Math.max(0, jobState.stats.pending - 1);
          jobState.stats.completed += 1;

          addJobLog(`Uploaded: "${uploadTitle}" ➔ ${youtubeUrl} (Unlisted)`, 'success');
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
          console.error(`Error uploading ${uploadTitle}:`, uploadErr);
          fileObj.status = 'failed';
          fileObj.error = uploadErr.message || 'Upload failed';

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

      if (jobState.status !== 'cancelled') {
        jobState.status = 'completed';
        jobState.finishedAt = new Date().toISOString();
        addJobLog('All videos in queue processed successfully.', 'success');
        broadcastSSE({ type: 'process_completed', message: 'All videos processed successfully.' });
        persistJobState();
      }

    } catch (fatalErr) {
      console.error('Fatal background error:', fatalErr);
      jobState.status = 'error';
      addJobLog(`Pipeline encountered fatal error: ${fatalErr.message}`, 'error');
      broadcastSSE({ type: 'error', message: fatalErr.message });
      persistJobState();
    } finally {
      activeAbortController = null;
    }
  })();
});

// Start Server
app.listen(PORT, () => {
  console.log(`====================================================`);
  console.log(` Drive-to-YouTube Background Streaming Service Live `);
  console.log(` Web UI: http://localhost:${PORT}                   `);
  console.log(` State File: ${STATE_FILE}                          `);
  console.log(`====================================================`);
});
