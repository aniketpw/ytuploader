#!/bin/bash
cd "$(dirname "$0")"

echo "========================================================"
echo "  🚀 DRIVE TO YOUTUBE UPLOADER - 1-CLICK LAUNCHER"
echo "========================================================"

# Check if node is installed
if ! command -v node &> /dev/null; then
    echo "❌ Node.js is not found! Please install Node.js."
    exit 1
fi

# Check if server is already running on port 3000
if lsof -i :3000 > /dev/null 2>&1; then
    echo "✔ Server is already running on port 3000."
else
    echo "⚡ Starting background server on port 3000..."
    nohup node server.js > /dev/null 2>&1 &
    sleep 1.5
fi

echo "🌐 Opening App in your default browser..."
open "http://localhost:3000"

echo ""
echo "✅ Plug & Play Ready! You can close this window now."
echo "========================================================"
