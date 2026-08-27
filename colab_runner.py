# ==============================================================================
# Google Drive to YouTube Cloud Streaming Engine
# ==============================================================================

import os
import subprocess
import time
import urllib.request
import re

print("Starting Drive-to-YouTube Streaming Portal...")

if not os.path.exists('/usr/local/bin/cloudflared'):
    urllib.request.urlretrieve(
        "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64",
        "/usr/local/bin/cloudflared"
    )
    os.chmod("/usr/local/bin/cloudflared", 0o777)

os.system("pkill -9 node; pkill -9 cloudflared")
subprocess.Popen(["node", "server.js"])
time.sleep(2)

tunnel = subprocess.Popen(
    ["cloudflared", "tunnel", "--url", "http://127.0.0.1:3000"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True
)

for line in tunnel.stderr:
    match = re.search(r'https://[a-zA-Z0-9-]+\.trycloudflare\.com', line)
    if match:
        url = match.group(0)
        print(f"\nYour Portal Link: {url}\n")
        break
