/**
 * Zero-Failure SQLite / JSON Hybrid Data Layer
 * Primary Engine: better-sqlite3 in WAL mode with compiled prepared statements & indexes.
 * Fallback Engine: Seamless in-memory & JSON file store if native addon compilation fails in cloud containers.
 * The server will NEVER crash on module load regardless of environment.
 */

'use strict';

const path = require('path');
const fs = require('fs');

const DATA_DIR = path.join(__dirname, 'data');
const DB_PATH = path.join(DATA_DIR, 'app.db');
const STATE_FILE = path.join(DATA_DIR, 'job_state.json');
const HISTORY_FILE = path.join(DATA_DIR, 'uploaded_history.json');
const SETTINGS_FILE = path.join(DATA_DIR, 'settings.json');
const CREDS_FILE = path.join(DATA_DIR, 'credentials.json');

// Ensure data directory exists
try {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
} catch (e) {}

let isSqlite = false;
let db = null;
let stmts = {};
let bulkInsertHistoryTx = null;

// ─── Attempt SQLite Initialization ───────────────────────────────────────────
try {
  const Database = require('better-sqlite3');
  db = new Database(DB_PATH);

  try {
    db.pragma('journal_mode = WAL');
  } catch (walErr) {
    try { db.pragma('journal_mode = DELETE'); } catch (e) {}
  }
  try { db.pragma('synchronous = NORMAL'); } catch (e) {}
  try { db.pragma('busy_timeout = 5000'); } catch (e) {}
  try { db.pragma('foreign_keys = ON'); } catch (e) {}

  db.exec(`
    CREATE TABLE IF NOT EXISTS uploaded_history (
      id            TEXT PRIMARY KEY,
      videoId       TEXT,
      name          TEXT,
      originalName  TEXT,
      customTitle   TEXT,
      batch         TEXT DEFAULT 'Batch',
      subject       TEXT DEFAULT 'Lecture',
      folderPath    TEXT DEFAULT '',
      channelId     TEXT,
      ownerUserId   TEXT,
      size          INTEGER DEFAULT 0,
      createdTime   TEXT,
      status        TEXT DEFAULT 'completed',
      percentage    INTEGER DEFAULT 100,
      uploadedBytes INTEGER DEFAULT 0,
      totalBytes    INTEGER DEFAULT 0,
      speedMBps     REAL DEFAULT 0,
      etaSeconds    INTEGER DEFAULT 0,
      youtubeUrl    TEXT DEFAULT '',
      thumbnailUrl  TEXT DEFAULT '',
      studioUrl     TEXT DEFAULT '',
      error         TEXT
    );

    CREATE TABLE IF NOT EXISTS job_state (
      id   INTEGER PRIMARY KEY CHECK (id = 1),
      data TEXT NOT NULL DEFAULT '{}'
    );

    CREATE TABLE IF NOT EXISTS settings (
      key   TEXT PRIMARY KEY,
      value TEXT
    );

    CREATE TABLE IF NOT EXISTS credentials (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      clientId       TEXT NOT NULL,
      clientSecret   TEXT NOT NULL,
      refreshToken   TEXT NOT NULL,
      label          TEXT DEFAULT 'Default',
      isActive       INTEGER DEFAULT 1,
      quotaUsedToday INTEGER DEFAULT 0,
      lastResetAt    TEXT
    );

    CREATE TABLE IF NOT EXISTS allowed_editors (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      email      TEXT UNIQUE NOT NULL COLLATE NOCASE,
      role       TEXT DEFAULT 'editor',
      addedBy    TEXT,
      createdAt  TEXT
    );

    CREATE TABLE IF NOT EXISTS otp_codes (
      email      TEXT PRIMARY KEY COLLATE NOCASE,
      code       TEXT NOT NULL,
      expiresAt  INTEGER NOT NULL,
      attempts   INTEGER DEFAULT 0,
      createdAt  TEXT
    );

    CREATE TABLE IF NOT EXISTS editor_sessions (
      token       TEXT PRIMARY KEY,
      email       TEXT NOT NULL COLLATE NOCASE,
      role        TEXT DEFAULT 'editor',
      channelId   TEXT,
      ownerUserId TEXT,
      createdAt   TEXT,
      expiresAt   INTEGER NOT NULL
    );
  `);

  try { db.exec('ALTER TABLE uploaded_history ADD COLUMN ownerUserId TEXT;'); } catch (e) {}
  try { db.exec('CREATE INDEX IF NOT EXISTS idx_history_ownerUserId ON uploaded_history(ownerUserId);'); } catch (e) {}
  try { db.exec('CREATE INDEX IF NOT EXISTS idx_history_videoId ON uploaded_history(videoId);'); } catch (e) {}
  try { db.exec('CREATE INDEX IF NOT EXISTS idx_history_channelId ON uploaded_history(channelId);'); } catch (e) {}
  try { db.exec('CREATE INDEX IF NOT EXISTS idx_history_name ON uploaded_history(name COLLATE NOCASE);'); } catch (e) {}
  try { db.exec('CREATE INDEX IF NOT EXISTS idx_history_customTitle ON uploaded_history(customTitle COLLATE NOCASE);'); } catch (e) {}
  try { db.exec('CREATE INDEX IF NOT EXISTS idx_editors_email ON allowed_editors(email COLLATE NOCASE);'); } catch (e) {}
  try { db.exec('CREATE INDEX IF NOT EXISTS idx_sessions_email ON editor_sessions(email COLLATE NOCASE);'); } catch (e) {}

  // Auto-deduplicate any existing history rows by videoId on startup
  try {
    db.exec(`
      DELETE FROM uploaded_history 
      WHERE rowid NOT IN (
        SELECT MIN(rowid) 
        FROM uploaded_history 
        GROUP BY COALESCE(NULLIF(videoId, ''), id)
      );
    `);
  } catch (e) {}

  stmts = {
    insertHistory: db.prepare(`
      INSERT OR REPLACE INTO uploaded_history
        (id, videoId, name, originalName, customTitle, batch, subject, folderPath,
         channelId, ownerUserId, size, createdTime, status, percentage, uploadedBytes, totalBytes,
         speedMBps, etaSeconds, youtubeUrl, thumbnailUrl, studioUrl, error)
      VALUES
        (@id, @videoId, @name, @originalName, @customTitle, @batch, @subject, @folderPath,
         @channelId, @ownerUserId, @size, @createdTime, @status, @percentage, @uploadedBytes, @totalBytes,
         @speedMBps, @etaSeconds, @youtubeUrl, @thumbnailUrl, @studioUrl, @error)
    `),
    selectAllHistory: db.prepare(`SELECT * FROM uploaded_history ORDER BY rowid DESC`),
    selectHistoryByChannel: db.prepare(`SELECT * FROM uploaded_history WHERE channelId = ? ORDER BY rowid DESC`),
    selectHistoryByUser: db.prepare(`SELECT * FROM uploaded_history WHERE ownerUserId = ? ORDER BY rowid DESC`),
    selectHistoryByChannelOrUser: db.prepare(`
      SELECT * FROM uploaded_history 
      WHERE (channelId IS NOT NULL AND channelId = ?) OR (ownerUserId IS NOT NULL AND ownerUserId = ?)
      ORDER BY rowid DESC
    `),
    selectHistoryById: db.prepare(`SELECT * FROM uploaded_history WHERE id = ?`),
    selectHistoryByVideoId: db.prepare(`SELECT * FROM uploaded_history WHERE videoId = ?`),
    deleteAllHistory: db.prepare(`DELETE FROM uploaded_history`),
    deleteHistoryById: db.prepare(`DELETE FROM uploaded_history WHERE id = ?`),
    deleteHistoryByChannel: db.prepare(`DELETE FROM uploaded_history WHERE channelId = ?`),
    deleteHistoryByUser: db.prepare(`DELETE FROM uploaded_history WHERE ownerUserId = ?`),
    deleteHistoryByChannelOrUser: db.prepare(`
      DELETE FROM uploaded_history 
      WHERE (channelId IS NOT NULL AND channelId = ?) OR (ownerUserId IS NOT NULL AND ownerUserId = ?)
    `),
    checkDuplicate: db.prepare(`
      SELECT id, videoId, youtubeUrl, customTitle, name FROM uploaded_history
      WHERE id = ? OR customTitle = ? COLLATE NOCASE OR name = ? COLLATE NOCASE
      LIMIT 1
    `),
    countHistoryInCycle: db.prepare(`
      SELECT COUNT(*) as cnt FROM uploaded_history
      WHERE createdTime >= ? AND channelId = ?
    `),
    countUserHistoryInCycle: db.prepare(`
      SELECT COUNT(*) as cnt FROM uploaded_history
      WHERE createdTime >= ? AND ownerUserId = ?
    `),
    countAllHistoryInCycle: db.prepare(`
      SELECT COUNT(*) as cnt FROM uploaded_history
      WHERE createdTime >= ?
    `),
    upsertJobState: db.prepare(`INSERT OR REPLACE INTO job_state (id, data) VALUES (1, ?)`),
    selectJobState: db.prepare(`SELECT data FROM job_state WHERE id = 1`),
    getSetting: db.prepare(`SELECT value FROM settings WHERE key = ?`),
    setSetting: db.prepare(`INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)`),
    getAllSettings: db.prepare(`SELECT key, value FROM settings`),
    deleteSetting: db.prepare(`DELETE FROM settings WHERE key = ?`),
    insertCredential: db.prepare(`
      INSERT INTO credentials (clientId, clientSecret, refreshToken, label, isActive)
      VALUES (?, ?, ?, ?, 1)
    `),
    selectActiveCredentials: db.prepare(`
      SELECT * FROM credentials WHERE isActive = 1 ORDER BY quotaUsedToday ASC
    `),
    selectAllCredentials: db.prepare(`SELECT id, label, isActive, quotaUsedToday, lastResetAt FROM credentials`),
    updateCredentialQuota: db.prepare(`UPDATE credentials SET quotaUsedToday = ? WHERE id = ?`),
    resetAllCredentialQuotas: db.prepare(`UPDATE credentials SET quotaUsedToday = 0, lastResetAt = ?`),
    deleteCredential: db.prepare(`DELETE FROM credentials WHERE id = ?`),
    selectCredentialById: db.prepare(`SELECT * FROM credentials WHERE id = ?`),

    selectAllEditors: db.prepare(`SELECT * FROM allowed_editors ORDER BY id DESC`),
    selectEditorByEmail: db.prepare(`SELECT * FROM allowed_editors WHERE email = ? COLLATE NOCASE`),
    insertEditor: db.prepare(`INSERT OR REPLACE INTO allowed_editors (email, role, addedBy, createdAt) VALUES (?, ?, ?, ?)`),
    deleteEditorByEmail: db.prepare(`DELETE FROM allowed_editors WHERE email = ? COLLATE NOCASE`),

    selectOtp: db.prepare(`SELECT * FROM otp_codes WHERE email = ? COLLATE NOCASE`),
    upsertOtp: db.prepare(`INSERT OR REPLACE INTO otp_codes (email, code, expiresAt, attempts, createdAt) VALUES (?, ?, ?, ?, ?)`),
    deleteOtp: db.prepare(`DELETE FROM otp_codes WHERE email = ? COLLATE NOCASE`),

    selectEditorSession: db.prepare(`SELECT * FROM editor_sessions WHERE token = ?`),
    insertEditorSession: db.prepare(`INSERT OR REPLACE INTO editor_sessions (token, email, role, channelId, ownerUserId, createdAt, expiresAt) VALUES (?, ?, ?, ?, ?, ?, ?)`),
    deleteEditorSession: db.prepare(`DELETE FROM editor_sessions WHERE token = ?`),
    deleteEditorSessionsByEmail: db.prepare(`DELETE FROM editor_sessions WHERE email = ? COLLATE NOCASE`)
  };

  bulkInsertHistoryTx = db.transaction((records) => {
    for (const rec of records) {
      stmts.insertHistory.run(normalizeHistoryRecord(rec));
    }
  });

  isSqlite = true;
  console.log('[db] SQLite WAL database initialized successfully at', DB_PATH);
} catch (err) {
  isSqlite = false;
  console.warn('[db] SQLite native engine unavailable, engaging Zero-Downtime JSON storage engine:', err.message);
}

// ─── Normalizer Helper ────────────────────────────────────────────────────────
function normalizeHistoryRecord(rec) {
  return {
    id: rec.id || '',
    videoId: rec.videoId || rec.id || '',
    name: rec.customTitle || rec.name || rec.originalName || '',
    originalName: rec.originalName || rec.name || '',
    customTitle: rec.customTitle || rec.name || '',
    batch: rec.batch || 'Batch',
    subject: rec.subject || 'Lecture',
    folderPath: rec.folderPath || '',
    channelId: rec.channelId || null,
    ownerUserId: rec.ownerUserId || null,
    size: parseInt(rec.size || rec.totalBytes || '0', 10),
    createdTime: rec.createdTime || new Date().toISOString(),
    status: rec.status || 'completed',
    percentage: rec.percentage != null ? rec.percentage : 100,
    uploadedBytes: parseInt(rec.uploadedBytes || rec.totalBytes || rec.size || '0', 10),
    totalBytes: parseInt(rec.totalBytes || rec.size || '0', 10),
    speedMBps: parseFloat(rec.speedMBps || '0'),
    etaSeconds: parseInt(rec.etaSeconds || '0', 10),
    youtubeUrl: rec.youtubeUrl || (rec.videoId ? `https://youtu.be/${rec.videoId}` : ''),
    thumbnailUrl: rec.thumbnailUrl || (rec.videoId ? `https://img.youtube.com/vi/${rec.videoId}/mqdefault.jpg` : ''),
    studioUrl: rec.studioUrl || (rec.videoId ? `https://studio.youtube.com/video/${rec.videoId}/edit` : ''),
    error: rec.error || null
  };
}

// ─── Fallback JSON Store Helpers ──────────────────────────────────────────────
function readJsonFile(file, def) {
  try {
    if (fs.existsSync(file)) return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (e) {}
  return def;
}

function writeJsonFile(file, data) {
  try {
    fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf8');
  } catch (e) {}
}

// ─── History Functions ────────────────────────────────────────────────────────
function loadUploadedHistory() {
  if (isSqlite) {
    try { return stmts.selectAllHistory.all(); } catch (err) {}
  }
  return readJsonFile(HISTORY_FILE, []);
}

function persistUploadedHistory(historyArray) {
  if (isSqlite) {
    try {
      if (!Array.isArray(historyArray) || historyArray.length === 0) {
        return;
      }
      bulkInsertHistoryTx(historyArray);
      return;
    } catch (err) {}
  }
  const existing = readJsonFile(HISTORY_FILE, []);
  const map = new Map();
  for (const item of existing) map.set(item.id || item.videoId, item);
  for (const item of (Array.isArray(historyArray) ? historyArray : [])) map.set(item.id || item.videoId, item);
  writeJsonFile(HISTORY_FILE, Array.from(map.values()));
}

function saveCompletedFileToHistory(fileObj) {
  if (!fileObj || (!fileObj.id && !fileObj.videoId)) return;
  if (isSqlite) {
    try {
      const vid = fileObj.videoId || (fileObj.id && fileObj.id.length === 11 ? fileObj.id : null);
      if (vid && vid.length === 11) {
        const existing = stmts.selectHistoryByVideoId.get(vid);
        if (existing) {
          const merged = {
            ...existing,
            ...fileObj,
            id: existing.id,
            customTitle: fileObj.customTitle || existing.customTitle || fileObj.name || existing.name,
            name: fileObj.name || existing.name || fileObj.customTitle || existing.customTitle,
            batch: (fileObj.batch && fileObj.batch !== 'Batch') ? fileObj.batch : (existing.batch || fileObj.batch || 'Batch'),
            subject: (fileObj.subject && fileObj.subject !== 'Lecture') ? fileObj.subject : (existing.subject || fileObj.subject || 'Lecture'),
            channelId: fileObj.channelId || existing.channelId || null,
            ownerUserId: fileObj.ownerUserId || existing.ownerUserId || null,
            thumbnailUrl: fileObj.thumbnailUrl || existing.thumbnailUrl || ''
          };
          stmts.insertHistory.run(normalizeHistoryRecord(merged));
          return;
        }
      }
      stmts.insertHistory.run(normalizeHistoryRecord(fileObj));
      return;
    } catch (err) {
      console.warn('[db] SQLite insertHistory error:', err.message);
    }
  }
  // JSON Fallback
  const history = readJsonFile(HISTORY_FILE, []);
  const vid = fileObj.videoId || (fileObj.id && fileObj.id.length === 11 ? fileObj.id : null);
  const idx = history.findIndex(f => (vid && f.videoId === vid) || f.id === fileObj.id);
  if (idx !== -1) {
    const existing = history[idx];
    history[idx] = {
      ...existing,
      ...fileObj,
      customTitle: fileObj.customTitle || existing.customTitle || fileObj.name || existing.name,
      name: fileObj.name || existing.name || fileObj.customTitle || existing.customTitle,
      batch: (fileObj.batch && fileObj.batch !== 'Batch') ? fileObj.batch : (existing.batch || fileObj.batch || 'Batch'),
      subject: (fileObj.subject && fileObj.subject !== 'Lecture') ? fileObj.subject : (existing.subject || fileObj.subject || 'Lecture'),
      channelId: fileObj.channelId || existing.channelId || null,
      ownerUserId: fileObj.ownerUserId || existing.ownerUserId || null,
      thumbnailUrl: fileObj.thumbnailUrl || existing.thumbnailUrl || ''
    };
  } else {
    history.push(normalizeHistoryRecord(fileObj));
  }
  writeJsonFile(HISTORY_FILE, history);
}

function deleteHistoryById(id) {
  if (isSqlite) {
    try { stmts.deleteHistoryById.run(id); return; } catch (err) {}
  }
  const hist = readJsonFile(HISTORY_FILE, []).filter(h => h.id !== id);
  writeJsonFile(HISTORY_FILE, hist);
}

function clearUserHistory(channelId, userId) {
  if (isSqlite) {
    try {
      if (channelId && userId) {
        stmts.deleteHistoryByChannelOrUser.run(channelId, userId);
      } else if (channelId) {
        stmts.deleteHistoryByChannel.run(channelId);
      } else if (userId) {
        stmts.deleteHistoryByUser.run(userId);
      }
      return;
    } catch (err) {}
  }
  const hist = readJsonFile(HISTORY_FILE, []).filter(h =>
    !(channelId && h.channelId === channelId) && !(userId && h.ownerUserId === userId)
  );
  writeJsonFile(HISTORY_FILE, hist);
}

function isDuplicate(driveFileId, customTitle, fileName) {
  if (isSqlite) {
    try {
      const row = stmts.checkDuplicate.get(driveFileId || '', (customTitle || '').trim(), (fileName || '').trim());
      return { isDuplicate: !!row, existing: row || null };
    } catch (err) {}
  }
  const hist = readJsonFile(HISTORY_FILE, []);
  const titleLow = (customTitle || '').trim().toLowerCase();
  const nameLow = (fileName || '').trim().toLowerCase();
  const row = hist.find(h =>
    (h.id && h.id === driveFileId) ||
    (h.customTitle && h.customTitle.trim().toLowerCase() === titleLow) ||
    (h.name && h.name.trim().toLowerCase() === nameLow)
  );
  return { isDuplicate: !!row, existing: row || null };
}

function findHistoryByDriveId(id) {
  if (isSqlite) {
    try { return stmts.selectHistoryById.get(id) || null; } catch (err) {}
  }
  return readJsonFile(HISTORY_FILE, []).find(h => h.id === id) || null;
}

function findHistoryByVideoId(videoId) {
  if (isSqlite) {
    try { return stmts.selectHistoryByVideoId.get(videoId) || null; } catch (err) {}
  }
  return readJsonFile(HISTORY_FILE, []).find(h => h.videoId === videoId) || null;
}

function getHistoryByChannel(channelId) {
  if (!channelId) return [];
  if (isSqlite) {
    try { return stmts.selectHistoryByChannel.all(channelId); } catch (err) {}
  }
  return readJsonFile(HISTORY_FILE, []).filter(h => h.channelId === channelId);
}

function getHistoryByUserOrChannel(channelId, userId) {
  if (isSqlite) {
    try {
      if (channelId && userId) {
        return stmts.selectHistoryByChannelOrUser.all(channelId, userId);
      } else if (channelId) {
        return stmts.selectHistoryByChannel.all(channelId);
      } else if (userId) {
        return stmts.selectHistoryByUser.all(userId);
      }
      return [];
    } catch (err) {}
  }
  const hist = readJsonFile(HISTORY_FILE, []);
  return hist.filter(h =>
    (channelId && h.channelId === channelId) || (userId && h.ownerUserId === userId)
  );
}

function getUploadsInCycle(sinceIso, channelId, userId) {
  if (!channelId && !userId) return 0;
  if (isSqlite) {
    try {
      if (channelId) {
        const row = stmts.countHistoryInCycle.get(sinceIso, channelId);
        return row ? row.cnt : 0;
      } else if (userId) {
        const row = stmts.countUserHistoryInCycle.get(sinceIso, userId);
        return row ? row.cnt : 0;
      }
      return 0;
    } catch (err) {}
  }
  const hist = readJsonFile(HISTORY_FILE, []);
  return hist.filter(f => {
    const created = f.createdTime || f.uploadedAt || f.timestamp;
    if (!created || created < sinceIso) return false;
    if (channelId && f.channelId === channelId) return true;
    if (userId && f.ownerUserId === userId) return true;
    return false;
  }).length;
}

// ─── Job State Functions ──────────────────────────────────────────────────────
function loadJobStateFromDB() {
  if (isSqlite) {
    try {
      const row = stmts.selectJobState.get();
      if (row && row.data) return JSON.parse(row.data);
    } catch (err) {}
  }
  return readJsonFile(STATE_FILE, null);
}

let _saveStateTimeout = null;
function persistJobStateToDB(state) {
  if (_saveStateTimeout) clearTimeout(_saveStateTimeout);
  _saveStateTimeout = setTimeout(() => {
    if (isSqlite) {
      try {
        stmts.upsertJobState.run(JSON.stringify(state));
        return;
      } catch (err) {}
    }
    writeJsonFile(STATE_FILE, state);
  }, 200);
}

function flushJobState(state) {
  if (_saveStateTimeout) clearTimeout(_saveStateTimeout);
  if (isSqlite) {
    try {
      stmts.upsertJobState.run(JSON.stringify(state));
      return;
    } catch (err) {}
  }
  writeJsonFile(STATE_FILE, state);
}

// ─── Settings Functions ───────────────────────────────────────────────────────
function getSetting(key) {
  if (isSqlite) {
    try {
      const row = stmts.getSetting.get(key);
      return row ? row.value : null;
    } catch (err) {}
  }
  return readJsonFile(SETTINGS_FILE, {})[key] || null;
}

function setSetting(key, value) {
  if (isSqlite) {
    try { stmts.setSetting.run(key, String(value)); return; } catch (err) {}
  }
  const s = readJsonFile(SETTINGS_FILE, {});
  s[key] = String(value);
  writeJsonFile(SETTINGS_FILE, s);
}

function getAllSettings() {
  if (isSqlite) {
    try {
      const rows = stmts.getAllSettings.all();
      const res = {};
      for (const r of rows) res[r.key] = r.value;
      return res;
    } catch (err) {}
  }
  return readJsonFile(SETTINGS_FILE, {});
}

function deleteSetting(key) {
  if (isSqlite) {
    try { stmts.deleteSetting.run(key); return; } catch (err) {}
  }
  const s = readJsonFile(SETTINGS_FILE, {});
  delete s[key];
  writeJsonFile(SETTINGS_FILE, s);
}

// ─── Credentials Functions ───────────────────────────────────────────────────
function addCredential(clientId, clientSecret, refreshToken, label) {
  if (isSqlite) {
    try { stmts.insertCredential.run(clientId, clientSecret, refreshToken, label || 'Default'); return; } catch (err) {}
  }
  const creds = readJsonFile(CREDS_FILE, []);
  creds.push({
    id: Date.now(),
    clientId,
    clientSecret,
    refreshToken,
    label: label || 'Default',
    isActive: 1,
    quotaUsedToday: 0
  });
  writeJsonFile(CREDS_FILE, creds);
}

function getActiveCredentials() {
  if (isSqlite) {
    try { return stmts.selectActiveCredentials.all(); } catch (err) {}
  }
  return readJsonFile(CREDS_FILE, []).filter(c => c.isActive === 1);
}

function getAllCredentials() {
  if (isSqlite) {
    try { return stmts.selectAllCredentials.all(); } catch (err) {}
  }
  return readJsonFile(CREDS_FILE, []).map(c => ({ id: c.id, label: c.label, isActive: c.isActive, quotaUsedToday: c.quotaUsedToday }));
}

function incrementCredentialQuota(credentialId) {
  if (isSqlite) {
    try {
      const cred = stmts.selectCredentialById.get(credentialId);
      if (cred) stmts.updateCredentialQuota.run((cred.quotaUsedToday || 0) + 1, credentialId);
      return;
    } catch (err) {}
  }
  const creds = readJsonFile(CREDS_FILE, []);
  const c = creds.find(x => x.id === credentialId);
  if (c) { c.quotaUsedToday = (c.quotaUsedToday || 0) + 1; writeJsonFile(CREDS_FILE, creds); }
}

function resetAllCredentialQuotas() {
  if (isSqlite) {
    try { stmts.resetAllCredentialQuotas.run(new Date().toISOString()); return; } catch (err) {}
  }
  const creds = readJsonFile(CREDS_FILE, []);
  creds.forEach(c => { c.quotaUsedToday = 0; });
  writeJsonFile(CREDS_FILE, creds);
}

function removeCredential(id) {
  if (isSqlite) {
    try { stmts.deleteCredential.run(id); return; } catch (err) {}
  }
  const creds = readJsonFile(CREDS_FILE, []).filter(c => c.id !== id);
  writeJsonFile(CREDS_FILE, creds);
}

function getCredentialById(id) {
  if (isSqlite) {
    try { return stmts.selectCredentialById.get(id) || null; } catch (err) {}
  }
  return readJsonFile(CREDS_FILE, []).find(c => c.id === id) || null;
}

// ─── Team / Editor Access Functions ──────────────────────────────────────────
const EDITORS_FILE = path.join(DATA_DIR, 'allowed_editors.json');
const OTP_FILE = path.join(DATA_DIR, 'otp_codes.json');
const SESSIONS_FILE = path.join(DATA_DIR, 'editor_sessions.json');

function getAllowedEditors() {
  if (isSqlite) {
    try { return stmts.selectAllEditors.all(); } catch (err) {}
  }
  return readJsonFile(EDITORS_FILE, []);
}

function isEditorAllowed(email) {
  if (!email || typeof email !== 'string') return false;
  const cleanEmail = email.trim().toLowerCase();
  if (isSqlite) {
    try {
      const row = stmts.selectEditorByEmail.get(cleanEmail);
      return !!row;
    } catch (err) {}
  }
  const editors = readJsonFile(EDITORS_FILE, []);
  return editors.some(e => (e.email || '').toLowerCase() === cleanEmail);
}

function addAllowedEditor(email, role = 'editor', addedBy = 'owner') {
  if (!email || typeof email !== 'string') return null;
  const cleanEmail = email.trim().toLowerCase();
  const now = new Date().toISOString();
  if (isSqlite) {
    try {
      stmts.insertEditor.run(cleanEmail, role, addedBy, now);
      return { email: cleanEmail, role, addedBy, createdAt: now };
    } catch (err) {
      console.warn('[db] Add editor SQLite error:', err.message);
    }
  }
  const editors = readJsonFile(EDITORS_FILE, []);
  const existingIdx = editors.findIndex(e => (e.email || '').toLowerCase() === cleanEmail);
  const rec = { id: Date.now(), email: cleanEmail, role, addedBy, createdAt: now };
  if (existingIdx >= 0) {
    editors[existingIdx] = rec;
  } else {
    editors.push(rec);
  }
  writeJsonFile(EDITORS_FILE, editors);
  return rec;
}

function removeAllowedEditor(email) {
  if (!email) return;
  const cleanEmail = email.trim().toLowerCase();
  if (isSqlite) {
    try {
      stmts.deleteEditorByEmail.run(cleanEmail);
      stmts.deleteEditorSessionsByEmail.run(cleanEmail);
      return;
    } catch (err) {}
  }
  const editors = readJsonFile(EDITORS_FILE, []).filter(e => (e.email || '').toLowerCase() !== cleanEmail);
  writeJsonFile(EDITORS_FILE, editors);
  const sessions = readJsonFile(SESSIONS_FILE, []).filter(s => (s.email || '').toLowerCase() !== cleanEmail);
  writeJsonFile(SESSIONS_FILE, sessions);
}

function saveOtpCode(email, code, ttlMinutes = 10) {
  if (!email || !code) return;
  const cleanEmail = email.trim().toLowerCase();
  const expiresAt = Date.now() + ttlMinutes * 60 * 1000;
  const now = new Date().toISOString();
  if (isSqlite) {
    try {
      stmts.upsertOtp.run(cleanEmail, String(code).trim(), expiresAt, 0, now);
      return { email: cleanEmail, code, expiresAt };
    } catch (err) {}
  }
  const otps = readJsonFile(OTP_FILE, {});
  otps[cleanEmail] = { code: String(code).trim(), expiresAt, attempts: 0, createdAt: now };
  writeJsonFile(OTP_FILE, otps);
  return { email: cleanEmail, code, expiresAt };
}

function verifyOtpCode(email, code) {
  if (!email || !code) return { valid: false, reason: 'Email and OTP are required.' };
  const cleanEmail = email.trim().toLowerCase();
  const inputCode = String(code).trim();
  let otpRecord = null;

  if (isSqlite) {
    try {
      otpRecord = stmts.selectOtp.get(cleanEmail);
    } catch (err) {}
  } else {
    const otps = readJsonFile(OTP_FILE, {});
    otpRecord = otps[cleanEmail];
  }

  if (!otpRecord) {
    return { valid: false, reason: 'No OTP requested for this email or OTP expired. Please request a new OTP.' };
  }

  if (Date.now() > otpRecord.expiresAt) {
    if (isSqlite) try { stmts.deleteOtp.run(cleanEmail); } catch (e) {}
    return { valid: false, reason: 'OTP has expired. Please request a new OTP.' };
  }

  if (otpRecord.code !== inputCode) {
    return { valid: false, reason: 'Incorrect OTP code. Please check and try again.' };
  }

  // Valid! Delete used OTP
  if (isSqlite) {
    try { stmts.deleteOtp.run(cleanEmail); } catch (e) {}
  } else {
    const otps = readJsonFile(OTP_FILE, {});
    delete otps[cleanEmail];
    writeJsonFile(OTP_FILE, otps);
  }

  return { valid: true };
}

function createEditorSession(email, role = 'editor', channelId = null, ownerUserId = null, ttlDays = 30) {
  const token = 'edt_' + Date.now().toString(36) + '_' + Math.random().toString(36).substr(2, 12);
  const cleanEmail = email.trim().toLowerCase();
  const expiresAt = Date.now() + ttlDays * 24 * 60 * 60 * 1000;
  const now = new Date().toISOString();

  if (isSqlite) {
    try {
      stmts.insertEditorSession.run(token, cleanEmail, role, channelId, ownerUserId, now, expiresAt);
      return { token, email: cleanEmail, role, channelId, ownerUserId, expiresAt };
    } catch (err) {}
  }
  const sessions = readJsonFile(SESSIONS_FILE, []);
  const rec = { token, email: cleanEmail, role, channelId, ownerUserId, createdAt: now, expiresAt };
  sessions.push(rec);
  writeJsonFile(SESSIONS_FILE, sessions);
  return rec;
}

function getEditorSession(token) {
  if (!token) return null;
  if (isSqlite) {
    try {
      const session = stmts.selectEditorSession.get(token);
      if (session && Date.now() < session.expiresAt) return session;
      return null;
    } catch (err) {}
  }
  const sessions = readJsonFile(SESSIONS_FILE, []);
  const s = sessions.find(item => item.token === token);
  if (s && Date.now() < s.expiresAt) return s;
  return null;
}

function deleteEditorSession(token) {
  if (!token) return;
  if (isSqlite) {
    try { stmts.deleteEditorSession.run(token); return; } catch (err) {}
  }
  const sessions = readJsonFile(SESSIONS_FILE, []).filter(s => s.token !== token);
  writeJsonFile(SESSIONS_FILE, sessions);
}

// ─── One-Time Migration ───────────────────────────────────────────────────────
function migrateFromJSON() {
  try {
    if (isSqlite) {
      const historyCount = db.prepare('SELECT COUNT(*) as cnt FROM uploaded_history').get().cnt;
      if (historyCount === 0 && fs.existsSync(HISTORY_FILE)) {
        const raw = fs.readFileSync(HISTORY_FILE, 'utf8');
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed) && parsed.length > 0) {
          bulkInsertHistoryTx(parsed);
          try { fs.copyFileSync(HISTORY_FILE, HISTORY_FILE + '.migrated'); } catch (e) {}
        }
      }
      const stateRow = stmts.selectJobState.get();
      if (!stateRow && fs.existsSync(STATE_FILE)) {
        const raw = fs.readFileSync(STATE_FILE, 'utf8');
        stmts.upsertJobState.run(raw);
        try { fs.copyFileSync(STATE_FILE, STATE_FILE + '.migrated'); } catch (e) {}
      }
    }

    // Seed credentials from env
    const existing = getActiveCredentials();
    if (existing.length === 0) {
      const cid = process.env.GOOGLE_CLIENT_ID;
      const csec = process.env.GOOGLE_CLIENT_SECRET;
      const ctok = process.env.GOOGLE_REFRESH_TOKEN;
      if (cid && csec && ctok) {
        addCredential(cid.trim(), csec.trim(), ctok.trim(), 'Primary (.env)');
      }
      const extra = process.env.GOOGLE_EXTRA_CREDENTIALS;
      if (extra) {
        extra.split(',').forEach((s, idx) => {
          const parts = s.split(':');
          if (parts.length >= 3) addCredential(parts[0].trim(), parts[1].trim(), parts[2].trim(), `Extra Key ${idx + 1}`);
        });
      }
    }

    if (!getSetting('upload_concurrency')) {
      setSetting('upload_concurrency', process.env.UPLOAD_CONCURRENCY || '3');
    }
  } catch (err) {
    console.warn('[db] Migration notice:', err.message);
  }
}

migrateFromJSON();

function closeDatabase() {
  try {
    if (_saveStateTimeout) clearTimeout(_saveStateTimeout);
    if (isSqlite && db) db.close();
  } catch (err) {}
}

module.exports = {
  loadUploadedHistory,
  persistUploadedHistory,
  saveCompletedFileToHistory,
  deleteHistoryById,
  clearUserHistory,
  isDuplicate,
  findHistoryByDriveId,
  findHistoryByVideoId,
  getHistoryByChannel,
  getHistoryByUserOrChannel,
  getUploadsInCycle,
  loadJobStateFromDB,
  persistJobStateToDB,
  flushJobState,
  getSetting,
  setSetting,
  getAllSettings,
  deleteSetting,
  addCredential,
  getActiveCredentials,
  getAllCredentials,
  getCredentialById,
  incrementCredentialQuota,
  resetAllCredentialQuotas,
  removeCredential,
  getAllowedEditors,
  isEditorAllowed,
  addAllowedEditor,
  removeAllowedEditor,
  saveOtpCode,
  verifyOtpCode,
  createEditorSession,
  getEditorSession,
  deleteEditorSession,
  closeDatabase,
  isSqliteActive: () => isSqlite,
  raw: db
};
