/**
 * One-Time OAuth2 Refresh Token Generator
 * Scopes: Google Drive (read-only) and YouTube Data API (upload)
 * 
 * Usage:
 *   node get-refresh-token.js
 */

require('dotenv').config();
const http = require('http');
const url = require('url');
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { google } = require('googleapis');

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

function prompt(query) {
  return new Promise((resolve) => rl.question(query, resolve));
}

const SCOPES = [
  'https://www.googleapis.com/auth/drive.readonly',
  'https://www.googleapis.com/auth/youtube.upload'
];

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
}

async function main() {
  console.log('\n======================================================');
  console.log('   Google OAuth2 Headless Refresh Token Generator     ');
  console.log('======================================================\n');

  let clientId = process.env.GOOGLE_CLIENT_ID;
  let clientSecret = process.env.GOOGLE_CLIENT_SECRET;
  let redirectUri = process.env.GOOGLE_REDIRECT_URI || 'http://localhost:3000/oauth2callback';

  if (!clientId || clientId.includes('your_google_client_id')) {
    clientId = await prompt('Enter your Google Client ID: ');
  }

  if (!clientSecret || clientSecret.includes('your_google_client_secret')) {
    clientSecret = await prompt('Enter your Google Client Secret: ');
  }

  if (!clientId || !clientSecret) {
    console.error('\n❌ Error: Client ID and Client Secret are required.');
    process.exit(1);
  }

  const parsedUrl = new URL(redirectUri);
  const port = parseInt(parsedUrl.port || '3000', 10);
  const pathname = parsedUrl.pathname || '/oauth2callback';

  const oauth2Client = new google.auth.OAuth2(clientId, clientSecret, redirectUri);

  // Generate Auth URL with offline access to get refresh token
  const authUrl = oauth2Client.generateAuthUrl({
    access_type: 'offline',
    prompt: 'consent', // Forces refresh token generation
    scope: SCOPES
  });

  console.log('\n👉 Step 1: Open the following URL in your browser and authorize the application:\n');
  console.log(`\x1b[36m${authUrl}\x1b[0m\n`);

  console.log(`Waiting for authorization callback on port ${port}...\n`);

  // Start local server to catch the callback
  const server = http.createServer(async (req, res) => {
    try {
      const parsedReqUrl = url.parse(req.url, true);

      if (parsedReqUrl.pathname === pathname) {
        const code = parsedReqUrl.query.code;

        if (!code) {
          res.writeHead(400, { 'Content-Type': 'text/html' });
          res.end('<h1>Authorization Failed: No code received.</h1>');
          return;
        }

        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(`
          <div style="font-family: sans-serif; text-align: center; padding: 50px; background: #0f172a; color: #f8fafc; min-height: 100vh;">
            <h1 style="color: #10b981; font-size: 28px;">✅ Authorization Successful!</h1>
            <p style="color: #94a3b8; font-size: 16px; margin-top: 10px;">The refresh token has been saved to your <code>.env</code> file.</p>
            <p style="color: #64748b; font-size: 14px;">You can close this tab and return to your terminal.</p>
          </div>
        `);

        // Exchange code for tokens
        const { tokens } = await oauth2Client.getToken(code);

        console.log('\n======================================================');
        console.log('                 TOKENS GENERATED                     ');
        console.log('======================================================\n');

        if (tokens.refresh_token) {
          console.log('\x1b[32m✔ Refresh Token obtained successfully!\x1b[0m\n');
          
          // Automatically save to .env
          updateEnvFile('GOOGLE_CLIENT_ID', clientId);
          updateEnvFile('GOOGLE_CLIENT_SECRET', clientSecret);
          updateEnvFile('GOOGLE_REDIRECT_URI', redirectUri);
          updateEnvFile('GOOGLE_REFRESH_TOKEN', tokens.refresh_token);

          console.log('✔ Automatically updated your \x1b[33m.env\x1b[0m file:');
          console.log(`GOOGLE_CLIENT_ID=${clientId}`);
          console.log(`GOOGLE_CLIENT_SECRET=${clientSecret}`);
          console.log(`GOOGLE_REDIRECT_URI=${redirectUri}`);
          console.log(`GOOGLE_REFRESH_TOKEN=${tokens.refresh_token}\n`);
          console.log('\x1b[32m🚀 You can now start the server with: npm start\x1b[0m\n');
        } else {
          console.log('\x1b[31m⚠ Warning: No refresh token returned.\x1b[0m');
          console.log('This happens if authorization was granted previously without prompt=consent.');
          console.log('Revoke app permissions in your Google Account security settings and re-run.\n');
        }

        server.close();
        rl.close();
        process.exit(0);
      }
    } catch (err) {
      console.error('Error exchanging authorization code:', err);
      res.writeHead(500, { 'Content-Type': 'text/html' });
      res.end(`<h1>Error: ${err.message}</h1>`);
      server.close();
      rl.close();
      process.exit(1);
    }
  });

  server.listen(port, '0.0.0.0', () => {
    // Server is listening
  });
}

main().catch(console.error);
