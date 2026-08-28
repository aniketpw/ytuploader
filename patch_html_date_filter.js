const fs = require('fs');
let code = fs.readFileSync('public/index.html', 'utf8');

const privacyPillFind = `      <!-- 6. Privacy Pill -->
      <div class="inline-flex items-center h-8 px-4 rounded border border-slate-300 bg-white font-semibold text-slate-700 shadow-2xs whitespace-nowrap">
        Privacy - <span class="ml-1 font-bold text-slate-900">Unlisted (100.00%)</span>
      </div>
    </div>`;

const privacyPillReplace = `      <!-- 6. Privacy Pill -->
      <div class="inline-flex items-center h-8 px-4 rounded border border-slate-300 bg-white font-semibold text-slate-700 shadow-2xs whitespace-nowrap">
        Privacy - <span class="ml-1 font-bold text-slate-900">Unlisted (100.00%)</span>
      </div>
      
      <div class="ml-auto flex items-center gap-2">
        <span class="text-xs font-bold text-slate-500 uppercase tracking-wider">Date:</span>
        <select id="tableDateFilter" onchange="setTableDateFilter(this.value)" class="h-8 px-3 rounded border border-slate-300 bg-white text-slate-700 text-xs font-bold shadow-2xs focus:outline-none focus:border-[#5046e5] cursor-pointer">
          <option value="all">All Dates</option>
        </select>
      </div>
    </div>`;

code = code.replace(privacyPillFind, privacyPillReplace);
fs.writeFileSync('public/index.html', code);
console.log('injected date dropdown');
