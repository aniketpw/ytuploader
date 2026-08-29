#!/bin/bash
cd "$(dirname "$0")"

echo "========================================================"
echo "  🚀 DRIVE TO YOUTUBE UPLOADER - DESKTOP APP (MAC)"
echo "========================================================"

# Check if node is installed
if ! command -v node &> /dev/null; then
    echo "❌ Node.js is not found! Please install Node.js."
    exit 1
fi

# Check if electron is available to open as native Desktop Window
if [ -d "node_modules/electron" ]; then
    echo "⚡ Opening Desktop App Window..."
    npx electron electron-main.js
else
    # Fallback to background server + browser
    if ! lsof -i :3000 > /dev/null 2>&1; then
        echo "⚡ Starting background server on port 3000..."
        nohup node server.js > /dev/null 2>&1 &
        sleep 1.5
    fi
    echo "🌐 Opening App in your default browser..."
    open "http://localhost:3000"
fi

echo "========================================================"
