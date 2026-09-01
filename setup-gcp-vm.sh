#!/bin/bash
set -e

echo "=========================================="
echo "🚀 Setting up Drive-to-YouTube on GCP VM"
echo "=========================================="

# 1. Update and install prerequisites
sudo apt-get update -y
sudo apt-get install -y curl git build-essential

# 2. Install Node.js 20 LTS
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs

# 3. Clone / Update Repository
if [ -d "ytuploader" ]; then
  echo "Updating existing ytuploader repository..."
  cd ytuploader
  git pull
else
  echo "Cloning ytuploader repository..."
  git clone https://github.com/aniketpw/ytuploader.git
  cd ytuploader
fi

# 4. Install Dependencies
echo "Installing project dependencies..."
npm install --omit=dev

# 5. Install PM2 process manager
sudo npm install -g pm2

# 6. Start / Restart App on Port 80 (HTTP)
sudo env PORT=80 pm2 start server.js --name "yt-uploader" || sudo pm2 restart yt-uploader
sudo pm2 save

echo "=========================================="
echo "✅ Setup Complete! App is running 24/7."
echo "Open http://<YOUR_VM_EXTERNAL_IP> in your browser"
echo "=========================================="
