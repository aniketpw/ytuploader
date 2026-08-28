const fs = require('fs');
let code = fs.readFileSync('server.js', 'utf8');

code = code.replace('app.use(express.json());', 'app.use(express.json({ limit: \'50mb\' })); // Increased for thumbnails');

fs.writeFileSync('server.js', code);
console.log('Express json limit increased');
