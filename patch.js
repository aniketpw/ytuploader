const fs = require('fs');
let code = fs.readFileSync('server.js', 'utf8');

const startIndex = code.indexOf('      for (let i = 0; i < jobState.files.length; i++) {');
if (startIndex === -1) {
  console.log("Could not find start index");
  process.exit(1);
}

const endIndex = code.indexOf(`      if (jobState.status !== 'cancelled') {
        jobState.status = 'completed';`);

if (endIndex === -1) {
  console.log("Could not find end index");
  process.exit(1);
}

const beforeLoop = code.slice(0, startIndex);
const loopBody = code.slice(startIndex, endIndex);
const afterLoop = code.slice(endIndex);

const newBeforeLoop = beforeLoop + `      await runUploadQueue(auth);\n\n`;

const modifiedAfterLoop = afterLoop.replace(
  `      if (jobState.status !== 'cancelled') {
        jobState.status = 'completed';
        jobState.finishedAt = new Date().toISOString();
        addJobLog('All videos in queue processed successfully.', 'success');
        broadcastSSE({ type: 'process_completed', message: 'All videos processed successfully.' });
        persistJobState();
      }

    } catch (fatalErr) {
      console.error('Fatal background error:', fatalErr);
      jobState.status = 'error';
      if (isAuthError(fatalErr)) {
        updateEnvFile('GOOGLE_REFRESH_TOKEN', '');
        addJobLog(\`Google Authentication Error: \${fatalErr.message}. Please re-connect Google account.\`, 'error');
        broadcastSSE({ type: 'auth_required', message: \`Authentication expired or invalid. Please click 'Connect Google' to authorize.\` });
      } else {
        addJobLog(\`Pipeline encountered fatal error: \${fatalErr.message}\`, 'error');
        broadcastSSE({ type: 'error', message: fatalErr.message });
      }
      persistJobState();
    } finally {
      activeAbortController = null;
    }
  })();
});`,
  `      if (jobState.status !== 'cancelled' && jobState.status !== 'paused_quota') {
        jobState.status = 'completed';
        jobState.finishedAt = new Date().toISOString();
        addJobLog('All videos in queue processed successfully.', 'success');
        broadcastSSE({ type: 'process_completed', message: 'All videos processed successfully.' });
        persistJobState();
      }

    } catch (fatalErr) {
      console.error('Fatal background error:', fatalErr);
      jobState.status = 'error';
      if (isAuthError(fatalErr)) {
        addJobLog(\`Google Authentication Error: \${fatalErr.message}. Please re-connect Google account.\`, 'error');
        broadcastSSE({ type: 'auth_required', message: \`Authentication expired or invalid. Please click 'Connect Google' to authorize.\` });
      } else {
        addJobLog(\`Pipeline encountered fatal error: \${fatalErr.message}\`, 'error');
        broadcastSSE({ type: 'error', message: fatalErr.message });
      }
      persistJobState();
    } finally {
      activeAbortController = null;
    }
  })();
});\n\n`
);

let runUploadQueueFunc = `
async function runUploadQueue(auth) {
  const { google } = require('googleapis');
  const drive = google.drive({ version: 'v3', auth });
  const youtube = google.youtube({ version: 'v3', auth });
  const { Transform } = require('stream');

` + loopBody + `
}
`;

// Insert the continue condition for completed files
runUploadQueueFunc = runUploadQueueFunc.replace(
  `        if (jobState.status === 'cancelled') {
          addJobLog('Pipeline cancelled during queue execution.', 'warn');
          break;
        }`,
  `        if (jobState.status === 'cancelled' || jobState.status === 'paused_quota') {
          addJobLog('Pipeline stopped during queue execution.', 'warn');
          break;
        }

        if (jobState.files[i].status === 'completed' || jobState.files[i].status === 'failed') {
          continue;
        }`
);

// We need to fix the resume endpoint to call runUploadQueue instead of processQueue
let newCode = newBeforeLoop + modifiedAfterLoop + runUploadQueueFunc;

newCode = newCode.replace(
  `  // Start processing again with the new auth
  processQueue(auth).catch(err => {`,
  `  // Start processing again with the new auth
  (async () => {
    try {
      activeAbortController = new AbortController();
      await runUploadQueue(auth);
      
      if (jobState.status !== 'cancelled' && jobState.status !== 'paused_quota') {
        jobState.status = 'completed';
        jobState.finishedAt = new Date().toISOString();
        addJobLog('All videos in queue processed successfully.', 'success');
        broadcastSSE({ type: 'process_completed', message: 'All videos processed successfully.' });
        persistJobState();
      }
    } catch(err) {`
);

// also fix the catch block in resume
newCode = newCode.replace(
  `  processQueue(auth).catch(err => {
    console.error('Background process queue error during resume:', err);
    jobState.status = 'error';
    addJobLog('Fatal error during background processing resume: ' + err.message, 'error');
    persistJobState();
    broadcastSSE({ type: 'error', message: err.message });
  });`,
  `  (async () => {
    try {
      activeAbortController = new AbortController();
      await runUploadQueue(auth);
      
      if (jobState.status !== 'cancelled' && jobState.status !== 'paused_quota') {
        jobState.status = 'completed';
        jobState.finishedAt = new Date().toISOString();
        addJobLog('All videos in queue processed successfully.', 'success');
        broadcastSSE({ type: 'process_completed', message: 'All videos processed successfully.' });
        persistJobState();
      }
    } catch(err) {
      console.error('Background process queue error during resume:', err);
      jobState.status = 'error';
      addJobLog('Fatal error during background processing resume: ' + err.message, 'error');
      persistJobState();
      broadcastSSE({ type: 'error', message: err.message });
    } finally {
      activeAbortController = null;
    }
  })();`
);


fs.writeFileSync('server.js', newCode);
console.log("Patched successfully");
