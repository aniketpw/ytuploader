const fs = require('fs');
let code = fs.readFileSync('public/index.html', 'utf8');

const searchAction = `        let actionHtml = '';
        if (embedUrl) {
          actionHtml = \`
            <div class="flex items-center justify-center gap-1.5">
              <a href="\${embedUrl}" target="_blank" rel="noopener noreferrer" class="w-8 h-8 rounded border border-indigo-200 bg-white hover:bg-indigo-50 text-[#5046e5] flex items-center justify-center transition-colors shadow-2xs cursor-pointer" title="Open YouTube Embed URL">
                <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"/>
                </svg>
              </a>
              <button onclick="copyToClipboard('\${embedUrl}', 'Embed Link')" class="w-8 h-8 rounded border border-slate-200 bg-slate-50 hover:bg-slate-100 text-slate-700 flex items-center justify-center transition-colors shadow-2xs cursor-pointer" title="Copy Embed URL">
                <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"/>
                </svg>
              </button>
              <a href="\${file.studioUrl || 'https://studio.youtube.com/video/' + file.videoId + '/edit'}" target="_blank" rel="noopener noreferrer" class="w-8 h-8 rounded border border-slate-200 bg-white hover:bg-amber-50 text-slate-600 hover:text-amber-700 flex items-center justify-center transition-colors shadow-2xs cursor-pointer" title="Open in YouTube Studio">
                <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15.232 5.232l3.536 3.536m-2.036-5.036a2.5 2.5 0 113.536 3.536L6.5 21.036H3v-3.572L16.732 3.732z" />
                </svg>
              </a>
            </div>
          \`;
        } else {
          actionHtml = \`<span class="text-[11px] text-slate-400 font-medium italic">Pending</span>\`;
        }`;

const replacementAction = `        let actionHtml = '';
        if (embedUrl) {
          if (currentState.processingMode === 'drive_secure') {
            actionHtml = \`
              <div class="flex items-center justify-center gap-1.5">
                <a href="\${embedUrl}" target="_blank" rel="noopener noreferrer" class="w-8 h-8 rounded border border-indigo-200 bg-white hover:bg-indigo-50 text-[#5046e5] flex items-center justify-center transition-colors shadow-2xs cursor-pointer" title="Open Drive Embed URL">
                  <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"/>
                  </svg>
                </a>
                <button onclick="copyToClipboard('\${embedUrl}', 'Embed Link')" class="w-8 h-8 rounded border border-slate-200 bg-slate-50 hover:bg-slate-100 text-slate-700 flex items-center justify-center transition-colors shadow-2xs cursor-pointer" title="Copy Embed URL">
                  <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"/>
                  </svg>
                </button>
              </div>
            \`;
          } else {
            actionHtml = \`
              <div class="flex items-center justify-center gap-1.5">
                <a href="\${embedUrl}" target="_blank" rel="noopener noreferrer" class="w-8 h-8 rounded border border-indigo-200 bg-white hover:bg-indigo-50 text-[#5046e5] flex items-center justify-center transition-colors shadow-2xs cursor-pointer" title="Open YouTube Embed URL">
                  <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"/>
                  </svg>
                </a>
                <button onclick="copyToClipboard('\${embedUrl}', 'Embed Link')" class="w-8 h-8 rounded border border-slate-200 bg-slate-50 hover:bg-slate-100 text-slate-700 flex items-center justify-center transition-colors shadow-2xs cursor-pointer" title="Copy Embed URL">
                  <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"/>
                  </svg>
                </button>
                <a href="\${file.studioUrl || 'https://studio.youtube.com/video/' + file.videoId + '/edit'}" target="_blank" rel="noopener noreferrer" class="w-8 h-8 rounded border border-slate-200 bg-white hover:bg-amber-50 text-slate-600 hover:text-amber-700 flex items-center justify-center transition-colors shadow-2xs cursor-pointer" title="Open in YouTube Studio">
                  <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15.232 5.232l3.536 3.536m-2.036-5.036a2.5 2.5 0 113.536 3.536L6.5 21.036H3v-3.572L16.732 3.732z" />
                  </svg>
                </a>
                <button onclick="triggerThumbnailUpload('\${file.videoId}')" class="w-8 h-8 rounded border border-slate-200 bg-white hover:bg-emerald-50 text-slate-600 hover:text-emerald-700 flex items-center justify-center transition-colors shadow-2xs cursor-pointer" title="Upload Custom Thumbnail">
                  <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z" />
                  </svg>
                </button>
              </div>
            \`;
          }
        } else {
          actionHtml = \`<span class="text-[11px] text-slate-400 font-medium italic">Pending</span>\`;
        }`;

code = code.replace(searchAction, replacementAction);

const searchFileType = `<div class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded bg-slate-100 text-slate-800 text-[11px] font-semibold border border-slate-200">
              <svg class="w-3.5 h-3.5 text-[#5046e5]" fill="currentColor" viewBox="0 0 24 24"><path d="M4 6.47v11.06c0 .8.88 1.28 1.54.84l8.29-5.53c.61-.41.61-1.27 0-1.68L5.54 5.63C4.88 5.19 4 5.67 4 6.47z"/></svg>
              <span>Video</span>
            </div>
            <div class="text-[11px] text-slate-500 font-mono mt-0.5">\${formatBytes(file.size || file.totalBytes)} (\${ext})</div>`;

const replaceFileType = `<div class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded bg-slate-100 text-slate-800 text-[11px] font-semibold border border-slate-200">
              <svg class="w-3.5 h-3.5 text-[#5046e5]" fill="currentColor" viewBox="0 0 24 24"><path d="M4 6.47v11.06c0 .8.88 1.28 1.54.84l8.29-5.53c.61-.41.61-1.27 0-1.68L5.54 5.63C4.88 5.19 4 5.67 4 6.47z"/></svg>
              <span>Video</span>
            </div>
            <div class="text-[11px] text-slate-500 font-mono mt-0.5">\${formatBytes(file.size || file.totalBytes)} (\${ext})</div>
            \${currentState.processingMode === 'drive_secure' 
              ? '<div class="mt-1.5 inline-flex items-center px-1.5 py-0.5 rounded bg-emerald-50 text-emerald-700 border border-emerald-200 text-[9px] font-bold">DRIVE PLAYER</div>'
              : '<div class="mt-1.5 inline-flex items-center px-1.5 py-0.5 rounded bg-red-50 text-red-700 border border-red-200 text-[9px] font-bold">YOUTUBE UPLOAD</div>'
            }`;

code = code.replace(searchFileType, replaceFileType);

const searchFuncs = `    window.copyToClipboard = function(text, label = 'Embed Link') {`;
const replaceFuncs = `    window.currentThumbnailVideoId = null;

    window.triggerThumbnailUpload = function(videoId) {
      window.currentThumbnailVideoId = videoId;
      document.getElementById('hiddenThumbInput').click();
    };

    window.handleThumbnailSelect = function(event) {
      const file = event.target.files[0];
      if (!file || !window.currentThumbnailVideoId) return;

      if (file.size > 10 * 1024 * 1024) {
        showToast('Thumbnail must be less than 10MB', 'error');
        return;
      }

      const reader = new FileReader();
      reader.onload = async (e) => {
        const base64 = e.target.result;
        showToast('Uploading thumbnail...', 'info');
        
        try {
          const res = await fetch('/api/thumbnail', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': 'Bearer ' + window.getAccessToken()
            },
            body: JSON.stringify({
              videoId: window.currentThumbnailVideoId,
              imageBase64: base64
            })
          });
          const data = await res.json();
          if (data.success) {
            showToast('Thumbnail updated successfully!', 'success');
            // Refresh table or wait for SSE
            loadJobStatus();
          } else {
            showToast('Failed to update thumbnail: ' + data.error, 'error');
          }
        } catch (err) {
          showToast('Error uploading thumbnail: ' + err.message, 'error');
        }
      };
      reader.readAsDataURL(file);
    };

    window.copyToClipboard = function(text, label = 'Embed Link') {`;

code = code.replace(searchFuncs, replaceFuncs);

const bodyEnd = `</body>`;
const inputHtml = `<input type="file" id="hiddenThumbInput" class="hidden" accept="image/jpeg, image/png, image/webp" onchange="handleThumbnailSelect(event)" />
</body>`;
code = code.replace(bodyEnd, inputHtml);

fs.writeFileSync('public/index.html', code);
console.log('UI Added');
