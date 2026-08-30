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

    CREATE INDEX IF NOT EXISTS idx_history_videoId     ON uploaded_history(videoId);
    CREATE INDEX IF NOT EXISTS idx_history_channelId   ON uploaded_history(channelId);
    CREATE INDEX IF NOT EXISTS idx_history_createdTime ON uploaded_history(createdTime);
    CREATE INDEX IF NOT EXISTS idx_history_name        ON uploaded_history(name COLLATE NOCASE);
    CREATE INDEX IF NOT EXISTS idx_history_customTitle ON uploaded_history(customTitle COLLATE NOCASE);

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
  `);

  stmts = {
    insertHistory: db.prepare(`
      INSERT OR REPLACE INTO uploaded_history
        (id, videoId, name, originalName, customTitle, batch, subject, folderPath,
         channelId, size, createdTime, status, percentage, uploadedBytes, totalBytes,
         speedMBps, etaSeconds, youtubeUrl, thumbnailUrl, studioUrl, error)
      VALUES
        (@id, @videoId, @name, @originalName, @customTitle, @batch, @subject, @folderPath,
         @channelId, @size, @createdTime, @status, @percentage, @uploadedBytes, @totalBytes,
         @speedMBps, @etaSeconds, @youtubeUrl, @thumbnailUrl, @studioUrl, @error)
    `),
    selectAllHistory: db.prepare(`SELECT * FROM uploaded_history ORDER BY rowid DESC`),
    selectHistoryByChannel: db.prepare(`SELECT * FROM uploaded_history WHERE channelId = ? ORDER BY rowid DESC`),
    selectHistoryById: db.prepare(`SELECT * FROM uploaded_history WHERE id = ?`),
    selectHistoryByVideoId: db.prepare(`SELECT * FROM uploaded_history WHERE videoId = ?`),
    deleteAllHistory: db.prepare(`DELETE FROM uploaded_history`),
    deleteHistoryById: db.prepare(`DELETE FROM uploaded_history WHERE id = ?`),
    checkDuplicate: db.prepare(`
      SELECT id, videoId, youtubeUrl, customTitle, name FROM uploaded_history
      WHERE id = ? OR customTitle = ? COLLATE NOCASE OR name = ? COLLATE NOCASE
      LIMIT 1
    `),
    countHistoryInCycle: db.prepare(`
      SELECT COUNT(*) as cnt FROM uploaded_history
      WHERE createdTime >= ? AND channelId = ?
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
    selectCredentialById: db.prepare(`SELECT * FROM credentials WHERE id = ?`)
  };

  bulkInsertHistoryTx = db.transaction((records) => {
    stmts.deleteAllHistory.run();
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
        stmts.deleteAllHistory.run();
        return;
      }
      bulkInsertHistoryTx(historyArray);
      return;
    } catch (err) {}
  }
  writeJsonFile(HISTORY_FILE, Array.isArray(historyArray) ? historyArray : []);
}

function saveCompletedFileToHistory(fileObj) {
  if (!fileObj || !fileObj.id) return;
  if (isSqlite) {
    try {
      stmts.insertHistory.run(normalizeHistoryRecord(fileObj));
      return;
    } catch (err) {}
  }
  const hist = readJsonFile(HISTORY_FILE, []);
  const idx = hist.findIndex(h => (h.videoId && h.videoId === fileObj.videoId) || h.id === fileObj.id);
  const rec = normalizeHistoryRecord(fileObj);
  if (idx >= 0) hist[idx] = { ...hist[idx], ...rec };
  else hist.unshift(rec);
  writeJsonFile(HISTORY_FILE, hist);
}

function deleteHistoryById(id) {
  if (isSqlite) {
    try { stmts.deleteHistoryById.run(id); return; } catch (err) {}
  }
  const hist = readJsonFile(HISTORY_FILE, []).filter(h => h.id !== id);
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

function getUploadsInCycle(sinceIso, channelId) {
  if (isSqlite) {
    try {
      if (channelId) {
        const row = stmts.countHistoryInCycle.get(sinceIso, channelId);
        return row ? row.cnt : 0;
      } else {
        const row = stmts.countAllHistoryInCycle.get(sinceIso);
        return row ? row.cnt : 0;
      }
    } catch (err) {}
  }
  const hist = readJsonFile(HISTORY_FILE, []);
  return hist.filter(f => {
    const created = f.createdTime || f.uploadedAt || f.timestamp;
    if (!created || created < sinceIso) return false;
    if (channelId && f.channelId !== channelId) return false;
    return true;
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
  isDuplicate,
  findHistoryByDriveId,
  findHistoryByVideoId,
  getHistoryByChannel,
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
  closeDatabase,
  isSqliteActive: () => isSqlite,
  raw: db
};
