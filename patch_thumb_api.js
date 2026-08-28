const fs = require('fs');
let code = fs.readFileSync('server.js', 'utf8');

const apiStr = `
/**
 * Update YouTube Thumbnail Endpoint
 */
app.post('/api/thumbnail', async (req, res) => {
  const auth = getOAuth2Client(req);
  if (!auth) {
    return res.status(401).json({ success: false, error: 'Google OAuth2 access token missing.' });
  }

  const { videoId, imageBase64 } = req.body;
  if (!videoId || !imageBase64) {
    return res.status(400).json({ success: false, error: 'Missing videoId or image data.' });
  }

  try {
    const { google } = require('googleapis');
    const youtube = google.youtube({ version: 'v3', auth });
    const { Readable } = require('stream');

    const base64Data = imageBase64.replace(/^data:image\\/\\w+;base64,/, '');
    const buffer = Buffer.from(base64Data, 'base64');
    
    // Guess mime type from base64 header if possible, else default to jpeg
    let mimeType = 'image/jpeg';
    if (imageBase64.startsWith('data:image/png')) mimeType = 'image/png';

    const readable = new Readable();
    readable._read = () => {};
    readable.push(buffer);
    readable.push(null);

    const result = await youtube.thumbnails.set({
      videoId: videoId,
      media: {
        mimeType: mimeType,
        body: readable
      }
    });

    res.json({ success: true, url: result.data.items[0].default.url });
  } catch (err) {
    console.error('Thumbnail upload error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// Start Server`;

code = code.replace('// Start Server', apiStr);
fs.writeFileSync('server.js', code);
console.log('API Added');
