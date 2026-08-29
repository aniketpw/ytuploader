@echo off
title Drive to YouTube Uploader - 1-Click Launcher
cd /d "%~dp0"

echo ========================================================
echo   🚀 DRIVE TO YOUTUBE UPLOADER - 1-CLICK LAUNCHER (WINDOWS)
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

:: 3. Start Node.js server in background if not already running
netstat -ano | findstr :3000 >nul
if %errorlevel% equ 0 (
    echo [*] Server is already running on port 3000.
) else (
    echo [*] Starting local server on http://localhost:3000 ...
    start /min "DriveToYouTubeServer" node server.js
    timeout /t 2 /nobreak >nul
)

:: 4. Open default browser
echo [*] Opening application in your browser...
start http://localhost:3000

echo.
echo [SUCCESS] App is running! You can minimize this window.
echo ========================================================
