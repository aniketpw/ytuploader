const { app, BrowserWindow, shell } = require('electron');
const path = require('path');
const http = require('http');

let mainWindow = null;
const PORT = process.env.PORT || 3000;

function waitForServer(url, timeoutMs = 15000) {
  const start = Date.now();
  return new Promise((resolve) => {
    function check() {
      const req = http.get(url, (res) => {
        if (res.statusCode >= 200 && res.statusCode < 500) {
          resolve(true);
        } else {
          retry();
        }
      });
      req.on('error', retry);
      req.end();
    }

    function retry() {
      if (Date.now() - start > timeoutMs) {
        return resolve(false);
      }
      setTimeout(check, 300);
    }

    check();
  });
}

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1380,
    height: 890,
    minWidth: 1024,
    minHeight: 700,
    title: 'Drive to YouTube Uploader',
    backgroundColor: '#0c0a09',
    autoHideMenuBar: true,
    show: false, // Show once ready to avoid white flash
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true
    }
  });

  // Load app URL
  mainWindow.loadURL(`http://localhost:${PORT}`);

  mainWindow.once('ready-to-show', () => {
    mainWindow.show();
  });

  // Open external links in default system browser
  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    if (url.startsWith('https://accounts.google.com') || url.startsWith('http://localhost')) {
      return { action: 'allow' };
    }
    shell.openExternal(url);
    return { action: 'deny' };
  });

  mainWindow.on('closed', () => {
    mainWindow = null;
  });
}

// Start backend Express server inside Electron
function startBackendServer() {
  try {
    require('./server.js');
  } catch (err) {
    console.error('Error starting internal server:', err);
  }
}

app.whenReady().then(async () => {
  startBackendServer();
  await waitForServer(`http://localhost:${PORT}`);
  createWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});
