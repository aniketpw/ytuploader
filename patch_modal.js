const fs = require('fs');
let code = fs.readFileSync('public/index.html', 'utf8');

const modalJsFind = `    window.openVideoModal = function(videoId, title, batch, subject) {
      if (!videoId) return;
      currentModalEmbedLink = \`https://www.youtube.com/embed/\${videoId}\`;
      modalIframe.src = \`https://www.youtube.com/embed/\${videoId}?autoplay=1\`;
      modalVideoTitle.textContent = title;
      modalBatchTag.textContent = batch || 'Batch';
      modalSubjectTag.textContent = subject || 'Subject';
      modalYtLink.href = \`https://www.youtube.com/watch?v=\${videoId}\`;
      videoModal.classList.remove('hidden');
    };`;

const modalJsReplace = `    window.openVideoModal = function(videoId, title, batch, subject, processingMode) {
      if (!videoId) return;
      
      const isDrive = processingMode === 'drive_secure';
      currentModalEmbedLink = isDrive 
        ? \`https://drive.google.com/file/d/\${videoId}/preview\`
        : \`https://www.youtube.com/embed/\${videoId}\`;
        
      modalIframe.src = isDrive 
        ? \`https://drive.google.com/file/d/\${videoId}/preview\`
        : \`https://www.youtube.com/embed/\${videoId}?autoplay=1\`;
        
      modalVideoTitle.textContent = title;
      modalBatchTag.textContent = batch || 'Batch';
      modalSubjectTag.textContent = subject || 'Subject';
      
      if (isDrive) {
        modalYtLink.href = \`https://drive.google.com/file/d/\${videoId}/view\`;
        modalYtLink.textContent = 'Open in Drive ↗';
      } else {
        modalYtLink.href = \`https://www.youtube.com/watch?v=\${videoId}\`;
        modalYtLink.textContent = 'Open Watch URL ↗';
      }
      
      videoModal.classList.remove('hidden');
    };`;
code = code.replace(modalJsFind, modalJsReplace);

const buttonFind = `<button onclick="openVideoModal('\${file.videoId}', '\${escapeHtml(displayTitle)}', '\${escapeHtml(file.batch)}', '\${escapeHtml(file.subject)}')" class="relative group w-16 h-10 rounded border border-slate-200 overflow-hidden shadow-2xs flex-shrink-0 bg-slate-900 cursor-pointer" title="Click to preview lecture">`;

const buttonReplace = `<button onclick="openVideoModal('\${file.videoId}', '\${escapeHtml(displayTitle)}', '\${escapeHtml(file.batch)}', '\${escapeHtml(file.subject)}', '\${currentState.processingMode}')" class="relative group w-16 h-10 rounded border border-slate-200 overflow-hidden shadow-2xs flex-shrink-0 bg-slate-900 cursor-pointer" title="Click to preview lecture">`;

code = code.replaceAll(buttonFind, buttonReplace);

fs.writeFileSync('public/index.html', code);
console.log('patched');
