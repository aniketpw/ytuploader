@echo off
title Drive to YouTube Uploader - Desktop App
cd /d "%~dp0"

echo ========================================================
echo   🚀 DRIVE TO YOUTUBE UPLOADER - DESKTOP APP (WINDOWS)
echo ========================================================

:: 1. Check if Node.js is installed
where node >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] Node.js is not installed!
    echo Please install Node.js from https://nodejs.org
    pause
    exit /b
)

:: 2. Check if node_modules exists, install if missing
if not exist "node_modules\" (
    echo [*] First time setup: Installing dependencies...
    call npm install
)

:: 3. Launch Desktop App Window
if exist "node_modules\electron\" (
    echo [*] Launching Native Desktop App Window...
    call npx electron electron-main.js
) else (
    :: Fallback to background server + browser
    netstat -ano | findstr :3000 >nul
    if %errorlevel% neq 0 (
        echo [*] Starting local server on http://localhost:3000 ...
        start /min "DriveToYouTubeServer" node server.js
        timeout /t 2 /nobreak >nul
    )
    echo [*] Opening application in your browser...
    start http://localhost:3000
)

echo ========================================================
