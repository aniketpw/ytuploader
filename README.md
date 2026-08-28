# 🎥 YouTube & Google Drive Automation Studio

A production-grade, zero-RAM streaming pipeline and video management studio built with **Node.js**, **Express**, **Tailwind CSS**, and **Server-Sent Events (SSE)**. 

Designed with a warm **Dark Editorial UI** (matching `officemobile.vercel.app`), this studio enables automated zero-RAM streaming from Google Drive to YouTube as **Unlisted** videos, plus a **Direct Local Device Upload Studio** with live in-browser preview, instant 16:9 thumbnail generator, and real-time percentage progress.

---

## ✨ Key Features & Capabilities

### 1. Dual Ingestion Pipelines
- **Google Drive Automated Pipeline:**
  - Zero-RAM chunk-by-chunk streaming via Node.js `stream.Transform` directly to YouTube.
  - Smart folder scanner with duplicate detection against previous upload history.
  - Date filtering (Today, Yesterday, Last 7 Days, or custom interactive Calendar range).
  - Customizable batch and subject lecture tagging.
- **Direct Manual Local Device Studio (`+ Manual Upload`):**
  - Drag-and-drop or file picker for local files (`.mp4`, `.mkv`, `.mov`, `.webm`, `.avi`).
  - **▶ Instant In-Browser Video Player Preview** before uploading.
  - Real-time 60fps upload progress (`0%` -> `100%`, transferred `MB/MB`, `Speed MB/s`, and remaining `ETA`).
  - Direct resumable session streaming to YouTube Data API v3.

### 2. Instant 16:9 Thumbnail Studio
- **⚡ 1-Click Smart Canvas Generator:** Automatically crafts branded 1280x720 HD thumbnails with batch labels, lecture titles, and styling.
- **Local Image Upload:** Choose PNG, JPG, or WebP files from your device.
- **Web / Drive Image Link:** Paste any Google Drive image link or web URL for auto-detection and upload.

### 3. Real-Time Streaming & Live Hero Banner
- Real-time Server-Sent Events (SSE) broadcasting live progress to all connected dashboard tabs.
- Pinned top banner showing live file name, speed, percentage bar, and ETA during active uploads.

### 4. Interactive Library & Management
- **Date Range Calendar Popover:** Visual date picker with quick presets (Today, Yesterday, Last 7 Days, All Dates).
- **Multi-Factor Search & Filtering:** Filter by status (`Completed`, `Uploading`, `Queued`, `Failed`), source (`YouTube`, `Drive`), or title/batch text.
- **Embedded Modal Player:** Watch uploaded videos or preview Drive files directly inside the application.
- **Metadata Editor:** Rename titles, adjust batch/subject tags, and replace thumbnails anytime.

### 5. Multi-User OAuth & 5-Step Guided Setup Wizard
- Built-in guided wizard to create your own Google Cloud project in 5 minutes.
- Connect your own Google account to get dedicated 10,000 units/day quota.

---

## ⚙️ How It Works Internally

The application leverages streaming architectures to process multi-gigabyte video files with minimal server resource consumption.

### 1. High-Level System Architecture

```mermaid
flowchart TD
    subgraph Client["Client Browser (Dark Editorial UI)"]
        UI["Dashboard & Table"]
        Player["In-Browser Video Player"]
        Studio["Direct Manual Upload Studio"]
        ThumbGen["HTML5 16:9 Canvas Generator"]
    end

    subgraph Server["Node.js Express Server"]
        Router["Express API Endpoints"]
        SSEHub["SSE Event Dispatcher"]
        TransformStream["Node.js Transform Progress Stream"]
        StateEngine["Job State & Upload History Engine"]
    end

    subgraph GoogleAPIs["Google Cloud Ecosystem"]
        DriveAPI["Google Drive API v3"]
        YouTubeAPI["YouTube Data API v3"]
        OAuthService["Google OAuth 2.0 Token Service"]
    end

    UI -->|"1. Scan / Upload Commands"| Router
    Studio -->|"2. Initiate Resumable Session"| Router
    Router -->|"3. Query Drive / Upload Chunks"| GoogleAPIs
    DriveAPI -->|"Stream Video (alt=media)"| TransformStream
    TransformStream -->|"Pipe Stream (Zero RAM)"| YouTubeAPI
    TransformStream -->|"Progress Events (Bytes, %)"| SSEHub
    SSEHub -->|"Live SSE Events"| UI
    StateEngine <-->|"Persist State & History"| Router
```

---

### 2. Google Drive to YouTube Streaming Pipeline (Zero-RAM)

```mermaid
sequenceDiagram
    autonumber
    actor User as User / Operator
    participant Browser as UI Dashboard (SSE Client)
    participant Server as Express Server
    participant State as Job State & History DB
    participant Drive as Google Drive API
    participant YouTube as YouTube Data API v3

    User->>Browser: Paste Drive Folder URL & Click Scan
    Browser->>Server: POST /api/scan-folder { folderInput, dateRange }
    Server->>Drive: drive.files.list(q: video files, createdTime)
    Drive-->>Server: Array of Discovered Video Files
    Server->>State: Check duplicate status against uploaded history
    Server-->>Browser: Return scanned files list with duplicate flags
    
    User->>Browser: Review selection & click "Start Upload"
    Browser->>Server: POST /api/process-selected { selectedFileIds, customTitles }
    
    loop For Each Selected Video
        Server->>Browser: SSE broadcast: file_start (title, size)
        Server->>Drive: drive.files.get(fileId, alt='media', responseType='stream')
        Drive-->>Server: Video Readable Stream
        
        Note over Server,YouTube: Piped chunk-by-chunk via stream.Transform (0 byte local disk / RAM storage)
        Server->>YouTube: youtube.videos.insert(part: 'snippet,status', media: transformStream, privacy: 'unlisted')
        
        loop During Chunk Transfer
            Server-->>Browser: SSE broadcast: upload_progress (bytes, %, speed, eta)
        end
        
        YouTube-->>Server: Upload Completed { videoId, snippet }
        
        opt If Custom / Smart Thumbnail Attached
            Server->>YouTube: youtube.thumbnails.set(videoId, thumbnailBuffer)
            YouTube-->>Server: Thumbnail Updated
        end
        
        Server->>State: Record file in job_state.json & uploaded_history.json
        Server-->>Browser: SSE broadcast: file_completed { videoId, youtubeUrl }
    end

    Server-->>Browser: SSE broadcast: process_completed
```

---

### 3. Direct Manual Local Device Upload Pipeline

```mermaid
sequenceDiagram
    autonumber
    actor User as User
    participant Modal as Manual Upload Studio (Modal)
    participant Server as Express Server
    participant YouTube as YouTube Data API v3

    User->>Modal: Drag & Drop Video (.mp4 / .mkv / .mov)
    Modal->>Modal: URL.createObjectURL(file) -> Instant Local Video Player Preview
    User->>Modal: Customize Title, Batch, Subject & 1-Click Smart Thumbnail
    User->>Modal: Click "Start Direct Upload"
    
    Modal->>Server: POST /api/initiate-direct-upload { title, description, privacy, size, mimeType }
    Server->>YouTube: Request Resumable Upload URI (youtube.videos.insert uploadType=resumable)
    YouTube-->>Server: Resumable Upload Session URL
    Server-->>Modal: Return uploadSessionUrl

    Note over Modal,YouTube: Browser streams file directly to YouTube endpoint in chunks
    loop Chunk Streaming
        Modal->>YouTube: PUT bytes to uploadSessionUrl
        Modal->>Modal: Update 60fps Real-Time Progress Bar (MB, %, Speed, ETA)
    end

    YouTube-->>Modal: 200 OK { id: videoId }
    Modal->>Server: POST /api/complete-direct-upload { videoId, title, batch, subject, thumbnailBase64 }
    
    opt If Thumbnail Provided
        Server->>YouTube: youtube.thumbnails.set(videoId, thumbnailBuffer)
        YouTube-->>Server: Thumbnail Applied
    end

    Server-->>Modal: Success { videoId, youtubeUrl }
    Modal-->>User: Show 100% Ready & Add to Library
```

---

## 📂 Project Structure

```
.
├── server.js                   # Express server, Google APIs streaming engine, SSE hub, & REST routes
├── get-refresh-token.js        # Interactive OAuth CLI utility to generate permanent refresh_token
├── public/
│   └── index.html              # Dark editorial frontend (Dashboard, Studio, Player, Modals, SSE client)
├── data/
│   ├── job_state.json          # Persistent active and completed video records
│   └── uploaded_history.json   # Persistent history used for smart duplicate detection
├── .env.example                # Environment variables template
├── package.json                # Project dependencies and startup scripts
└── README.md                   # System documentation and architecture guide
```

---

## 🚀 Installation & Quick Start

### 1. Clone & Install Dependencies

```bash
git clone https://github.com/aniketpw/ytuploader.git
cd ytuploader
npm install
```

### 2. Configure Environment Variables

Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

Edit `.env` with your Google OAuth credentials:

```env
PORT=3000
GOOGLE_CLIENT_ID=your_client_id.apps.googleusercontent.com
GOOGLE_CLIENT_SECRET=GOCSPX-your_client_secret
GOOGLE_REDIRECT_URI=http://localhost:3000/oauth2callback
GOOGLE_REFRESH_TOKEN=1//your_permanent_refresh_token
```

*(Note: You can also use the in-app 5-step guided wizard to generate your OAuth Client ID and sign in directly from the UI without manual CLI setup).*

### 3. Generate OAuth Refresh Token (CLI Method)

```bash
npm run get-token
```
Follow the URL printed in the terminal, grant YouTube and Drive permissions, and paste the resulting refresh token into `.env`.

### 4. Start the Application

```bash
# Start production server
npm start

# Or start with nodemon development mode
npm run dev
```

Open your browser at:
```
http://localhost:3000
```

---

## 🔐 Google Cloud Console Configuration (Summary)

1. **APIs to Enable:**
   - Google Drive API
   - YouTube Data API v3
2. **OAuth Scopes Required:**
   - `https://www.googleapis.com/auth/drive.readonly`
   - `https://www.googleapis.com/auth/youtube.upload`
   - `https://www.googleapis.com/auth/youtube`
3. **Authorized JavaScript Origins:**
   - `http://localhost:3000` (or your production deployment domain)
4. **Authorized Redirect URIs:**
   - `http://localhost:3000/oauth2callback`

---

## 🛡️ License

Private and proprietary. Designed for automated Drive-to-YouTube video streaming and channel operations.
